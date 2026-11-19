//! Reconciliation of a single `TimeBasedAutoscaler`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jiff::Timestamp;
use jiff::tz::TimeZone;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType, Recorder};
use kube::{Client, Resource, ResourceExt};
use serde_json::json;

use crate::crd::{CONDITION_READY, CONTROLLER_NAME, TimeBasedAutoscaler, TimeBasedAutoscalerStatus};
use crate::error::Error;
use crate::metrics::{Direction, Metrics, Outcome};
use crate::schedule::CompiledSchedule;
use crate::target::{self, Resolver, TargetError};

/// Source of the current time.
#[derive(Debug, Clone, Copy, Default)]
pub enum Clock {
    /// The system clock.
    #[default]
    System,
    /// A fixed instant, for tests and offline evaluation.
    Fixed(Timestamp),
}

impl Clock {
    /// Current instant.
    #[must_use]
    pub fn now(self) -> Timestamp {
        match self {
            Self::System => Timestamp::now(),
            Self::Fixed(now) => now,
        }
    }
}

/// Shared state of all reconciliations.
pub struct Context {
    /// Kubernetes client.
    pub client: Client,
    /// Time zone schedules are evaluated in.
    pub tz: TimeZone,
    /// Maximum delay between two reconciliations of an object.
    pub resync: Duration,
    /// Delay before retrying a failed reconciliation.
    pub retry: Duration,
    /// Time source.
    pub clock: Clock,
    /// Event recorder.
    pub recorder: Recorder,
    /// Target discovery cache.
    pub resolver: Resolver,
    /// Process metrics.
    pub metrics: Arc<Metrics>,
}

/// Status computed by one pass, with the follow-up action.
struct Pass {
    ready: Condition,
    status: TimeBasedAutoscalerStatus,
    action: Action,
}

/// Reconciles one object: evaluates its schedule, enforces the floor and
/// records the observed state.
///
/// # Errors
///
/// Fails when the status cannot be written; target failures are reported in
/// the `Ready` condition instead.
pub async fn reconcile(tba: Arc<TimeBasedAutoscaler>, ctx: Arc<Context>) -> Result<Action, Error> {
    let started = Instant::now();
    let now = ctx.clock.now();
    let previous = tba.status.clone().unwrap_or_default();
    let mut pass = evaluate(&tba, &ctx, now, &previous).await;
    pass.status.observed_generation = tba.metadata.generation;
    pass.ready.observed_generation = tba.metadata.generation;
    pass.status.conditions = merge_condition(&previous.conditions, pass.ready, now);

    let outcome = if pass.status.conditions[0].status == "True" { Outcome::Success } else { Outcome::Failure };
    if tba.status.as_ref() != Some(&pass.status) {
        let api: Api<TimeBasedAutoscaler> = Api::namespaced(ctx.client.clone(), &tba.namespace().unwrap_or_default());
        let patch = Patch::Merge(json!({ "status": pass.status }));
        let params = PatchParams { field_manager: Some(CONTROLLER_NAME.into()), ..PatchParams::default() };
        api.patch_status(&tba.name_any(), &params, &patch).await?;
    }
    ctx.metrics.reconciled(outcome);
    ctx.metrics.reconcile_duration.observe(started.elapsed().as_secs_f64());
    Ok(pass.action)
}

/// Requeue policy for failed reconciliations.
#[must_use]
#[expect(clippy::needless_pass_by_value, reason = "signature required by kube::runtime::Controller::run")]
pub fn error_policy(tba: Arc<TimeBasedAutoscaler>, error: &Error, ctx: Arc<Context>) -> Action {
    tracing::warn!(namespace = tba.namespace(), name = tba.name_any(), %error, "reconciliation failed");
    ctx.metrics.reconciled(Outcome::Failure);
    Action::requeue(ctx.retry)
}

async fn evaluate(
    tba: &TimeBasedAutoscaler,
    ctx: &Context,
    now: Timestamp,
    previous: &TimeBasedAutoscalerStatus,
) -> Pass {
    let compiled = match CompiledSchedule::compile(&tba.spec) {
        Ok(compiled) => compiled,
        Err(error) => {
            publish(ctx, tba, EventType::Warning, "InvalidSchedule", error.to_string()).await;
            return Pass {
                ready: condition("False", "InvalidSchedule", error.to_string()),
                status: previous.clone(),
                action: Action::await_change(),
            };
        }
    };
    let evaluation = compiled.evaluate(now, &ctx.tz);
    let mut status = TimeBasedAutoscalerStatus {
        desired_min_replicas: Some(evaluation.floor),
        active_schedules: compiled.active_at(evaluation.minute),
        next_transition_time: evaluation.next_change.map(Time),
        managed_replicas: previous.managed_replicas,
        last_scale_time: previous.last_scale_time.clone(),
        ..TimeBasedAutoscalerStatus::default()
    };

    let target = &tba.spec.scale_target_ref;
    let namespace = tba.namespace().unwrap_or_default();
    let result = match ctx.resolver.resolve(&ctx.client, target).await {
        Ok(resolved) => {
            target::apply(&ctx.client, &namespace, target, &resolved, evaluation.floor, previous.managed_replicas).await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(applied) => {
            status.managed_replicas = applied.managed_replicas;
            if let Some((from, to)) = applied.change {
                status.last_scale_time = Some(Time(now));
                ctx.metrics.target_updated(if to > from { Direction::Up } else { Direction::Down });
                let note =
                    format!("{} {} changed from {from} to {to} (floor {})", target.kind, target.name, evaluation.floor);
                publish(ctx, tba, EventType::Normal, "FloorApplied", note).await;
            }
            let (reason, message) = applied.clamped.map_or_else(
                || ("FloorApplied", format!("floor {} enforced on {} {}", applied.enforced, target.kind, target.name)),
                |clamped| ("FloorClamped", clamped),
            );
            let wake = [evaluation.next_change, ctx.tz.following(now).next().map(|t| t.timestamp())]
                .into_iter()
                .flatten()
                .min();
            Pass {
                ready: condition("True", reason, message),
                status,
                action: Action::requeue(delay(now, wake, ctx.resync)),
            }
        }
        Err(error) => {
            let reason = error.reason();
            publish(ctx, tba, EventType::Warning, reason, error.to_string()).await;
            let retry = if matches!(error, TargetError::Read { .. } | TargetError::Write { .. }) {
                ctx.retry
            } else {
                ctx.resync
            };
            Pass { ready: condition("False", reason, error.to_string()), status, action: Action::requeue(retry) }
        }
    }
}

/// Time until `wake`, bounded by `resync`.
fn delay(now: Timestamp, wake: Option<Timestamp>, resync: Duration) -> Duration {
    wake.and_then(|wake| Duration::try_from(wake.duration_since(now)).ok()).map_or(resync, |until| until.min(resync))
}

fn condition(status: &str, reason: &str, message: String) -> Condition {
    Condition {
        type_: CONDITION_READY.into(),
        status: status.into(),
        reason: reason.into(),
        message,
        observed_generation: None,
        last_transition_time: Time(Timestamp::UNIX_EPOCH),
    }
}

/// Replaces the `Ready` condition, keeping its transition time while the
/// status is unchanged.
fn merge_condition(previous: &[Condition], mut ready: Condition, now: Timestamp) -> Vec<Condition> {
    ready.last_transition_time = previous
        .iter()
        .find(|c| c.type_ == ready.type_ && c.status == ready.status)
        .map_or(Time(now), |c| c.last_transition_time.clone());
    vec![ready]
}

async fn publish(ctx: &Context, tba: &TimeBasedAutoscaler, type_: EventType, reason: &str, note: String) {
    let event = Event { type_, reason: reason.into(), note: Some(note), action: "Reconcile".into(), secondary: None };
    if let Err(error) = ctx.recorder.publish(&event, &tba.object_ref(&())).await {
        tracing::warn!(%error, "publishing event failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::Weekday;
    use crate::fake_api::FakeApi;
    use kube::runtime::events::Reporter;
    use serde_json::Value;

    const TBA: &str = "/apis/tba.alexandernicholson.github.io/v1alpha1/namespaces/default/timebasedautoscalers/web";
    const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/default/deployments/web";
    const HPA: &str = "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers/web";
    const SCALED_OBJECT: &str = "/apis/keda.sh/v1alpha1/namespaces/default/scaledobjects/web";
    /// Monday 2026-01-05, inside business hours (08:00-18:00 UTC).
    const NOON: &str = "2026-01-05T12:00:00Z";
    /// Monday 2026-01-05, after business hours.
    const EVENING: &str = "2026-01-05T20:00:00Z";
    const RESYNC: Duration = Duration::from_hours(24);
    const RETRY: Duration = Duration::from_secs(15);

    fn context(api: &FakeApi, now: &str) -> Arc<Context> {
        let client = api.client();
        Arc::new(Context {
            recorder: Recorder::new(client.clone(), Reporter { controller: CONTROLLER_NAME.into(), instance: None }),
            client,
            tz: TimeZone::UTC,
            resync: RESYNC,
            retry: RETRY,
            clock: Clock::Fixed(now.parse().unwrap()),
            resolver: Resolver::default(),
            metrics: Arc::default(),
        })
    }

    fn insert_tba(api: &FakeApi, api_version: &str, kind: &str, business_floor: i32) {
        let days: Vec<Weekday> = Weekday::all().into_iter().take(5).collect();
        api.insert(
            TBA,
            json!({
                "apiVersion": "tba.alexandernicholson.github.io/v1alpha1", "kind": "TimeBasedAutoscaler",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "scaleTargetRef": {"apiVersion": api_version, "kind": kind, "name": "web"},
                    "minReplicas": 1,
                    "schedules": [{"name": "business", "days": days, "start": "08:00", "end": "18:00", "minReplicas": business_floor}]
                }
            }),
        );
    }

    fn insert_deployment(api: &FakeApi, replicas: i32) {
        api.insert(
            DEPLOYMENT,
            json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web", "namespace": "default"}, "spec": {"replicas": replicas}}),
        );
    }

    async fn fetch(api: &FakeApi) -> Arc<TimeBasedAutoscaler> {
        Arc::new(Api::<TimeBasedAutoscaler>::namespaced(api.client(), "default").get("web").await.unwrap())
    }

    /// Reconciles the stored object and returns the action and the stored status.
    async fn reconcile_once(api: &FakeApi, ctx: &Arc<Context>) -> (Action, TimeBasedAutoscalerStatus) {
        let action = reconcile(fetch(api).await, Arc::clone(ctx)).await.unwrap();
        (action, fetch(api).await.status.clone().unwrap())
    }

    fn replicas(api: &FakeApi, path: &str, field: &str) -> Value {
        api.get(path).unwrap()["spec"][field].clone()
    }

    fn ready(status: &TimeBasedAutoscalerStatus) -> (&str, &str) {
        (status.conditions[0].status.as_str(), status.conditions[0].reason.as_str())
    }

    #[tokio::test]
    async fn scale_target_is_raised_and_only_owned_replicas_are_released() {
        let api = FakeApi::start().await;
        insert_tba(&api, "apps/v1", "Deployment", 5);
        insert_deployment(&api, 1);
        let noon = context(&api, NOON);

        let (action, status) = reconcile_once(&api, &noon).await;
        assert_eq!(action, Action::requeue(Duration::from_hours(6)));
        assert_eq!(replicas(&api, DEPLOYMENT, "replicas"), 5);
        assert_eq!(status.desired_min_replicas, Some(5));
        assert_eq!(status.active_schedules, ["business"]);
        assert_eq!(status.next_transition_time, Some(Time("2026-01-05T18:00:00Z".parse().unwrap())));
        assert_eq!(status.managed_replicas, Some(5));
        assert_eq!(status.last_scale_time, Some(Time(NOON.parse().unwrap())));
        assert_eq!(status.observed_generation, Some(1));
        assert_eq!(ready(&status), ("True", "FloorApplied"));
        assert_eq!(status.conditions[0].observed_generation, Some(1));
        assert_eq!(api.count("POST /apis/events.k8s.io/v1/namespaces/default/events"), 1);

        // Nothing changed: neither the target nor the status is written again.
        let writes = api.count("PATCH");
        reconcile_once(&api, &noon).await;
        assert_eq!(api.count("PATCH"), writes);
        assert_eq!(api.count("GET /apis/apps/v1"), 3, "discovery is cached: {:?}", api.requests());

        // The floor drops and the controller owns the replicas: scale down.
        let (_, status) = reconcile_once(&api, &context(&api, EVENING)).await;
        assert_eq!(replicas(&api, DEPLOYMENT, "replicas"), 1);
        assert_eq!(status.managed_replicas, Some(1));
        assert_eq!(status.conditions[0].last_transition_time, Time(NOON.parse().unwrap()));

        // Someone else scaled up: the controller leaves the replicas alone.
        insert_deployment(&api, 8);
        let (_, status) = reconcile_once(&api, &context(&api, EVENING)).await;
        assert_eq!(replicas(&api, DEPLOYMENT, "replicas"), 8);
        assert_eq!(status.managed_replicas, None);
        assert_eq!(noon.metrics.encode().matches("outcome=\"success\"} 2").count(), 1);
    }

    #[tokio::test]
    async fn autoscaler_minimums_follow_the_floor() {
        let api = FakeApi::start().await;
        insert_tba(&api, "autoscaling/v2", "HorizontalPodAutoscaler", 5);
        api.insert(HPA, json!({"metadata": {"name": "web", "namespace": "default"}, "spec": {"maxReplicas": 10}}));
        let (_, status) = reconcile_once(&api, &context(&api, NOON)).await;
        assert_eq!(replicas(&api, HPA, "minReplicas"), 5);
        assert_eq!(ready(&status), ("True", "FloorApplied"));
        assert_eq!(status.managed_replicas, None);
        reconcile_once(&api, &context(&api, NOON)).await;
        assert_eq!(api.count(&format!("PATCH {HPA}")), 1, "an HPA already at the floor is not written");

        reconcile_once(&api, &context(&api, EVENING)).await;
        assert_eq!(replicas(&api, HPA, "minReplicas"), 1);

        insert_tba(&api, "autoscaling/v2", "HorizontalPodAutoscaler", 20);
        let (_, status) = reconcile_once(&api, &context(&api, NOON)).await;
        assert_eq!(replicas(&api, HPA, "minReplicas"), 10);
        assert_eq!(ready(&status), ("True", "FloorClamped"));
        assert_eq!(status.conditions[0].message, "floor 20 exceeds maxReplicas 10");

        insert_tba(&api, "keda.sh/v1alpha1", "ScaledObject", 5);
        api.insert(SCALED_OBJECT, json!({"metadata": {"name": "web", "namespace": "default"}, "spec": {}}));
        reconcile_once(&api, &context(&api, NOON)).await;
        assert_eq!(replicas(&api, SCALED_OBJECT, "minReplicaCount"), 5);
    }

    #[tokio::test]
    async fn target_problems_are_reported_in_the_ready_condition() {
        let api = FakeApi::start().await;
        let ctx = context(&api, NOON);

        insert_tba(&api, "apps/v1", "Deployment", 5);
        let (action, status) = reconcile_once(&api, &ctx).await;
        assert_eq!(action, Action::requeue(RESYNC));
        assert_eq!(ready(&status), ("False", "TargetNotFound"));
        assert_eq!(status.desired_min_replicas, Some(5));

        insert_tba(&api, "example.com/v1", "Widget", 5);
        let (_, status) = reconcile_once(&api, &ctx).await;
        assert_eq!(ready(&status), ("False", "UnsupportedTarget"));
        assert_eq!(status.conditions[0].message, "example.com/v1/Widget is not served by the API server");

        insert_tba(&api, "v1", "ConfigMap", 5);
        let (_, status) = reconcile_once(&api, &ctx).await;
        assert_eq!(status.conditions[0].message, "v1/ConfigMap does not expose the scale subresource");

        insert_tba(&api, "apps/v1", "Deployment", 5);
        insert_deployment(&api, 1);
        api.fail("GET", "/deployments/web/scale", 500, 1);
        let (action, status) = reconcile_once(&api, &ctx).await;
        assert_eq!(action, Action::requeue(RETRY));
        assert_eq!(ready(&status), ("False", "FailedGetTarget"));

        api.fail("PATCH", "/deployments/web/scale", 500, 1);
        let (_, status) = reconcile_once(&api, &ctx).await;
        assert_eq!(ready(&status), ("False", "FailedUpdateTarget"));
        assert_eq!(ctx.metrics.encode().matches("outcome=\"failure\"} 5").count(), 1);
    }

    #[tokio::test]
    async fn invalid_schedules_wait_for_a_spec_change() {
        let api = FakeApi::start().await;
        insert_tba(&api, "apps/v1", "Deployment", 5);
        let mut tba = (*fetch(&api).await).clone();
        tba.spec.schedules[0].start = "8am".into();
        let action = reconcile(Arc::new(tba), context(&api, NOON)).await.unwrap();
        assert_eq!(action, Action::await_change());
        let status = fetch(&api).await.status.clone().unwrap();
        assert_eq!(ready(&status), ("False", "InvalidSchedule"));
        assert_eq!(status.desired_min_replicas, None);
    }

    #[tokio::test]
    async fn status_write_failures_are_retried() {
        let api = FakeApi::start().await;
        insert_tba(&api, "apps/v1", "Deployment", 5);
        insert_deployment(&api, 1);
        api.fail("POST", "/events", 500, 1);
        api.fail("PATCH", "/timebasedautoscalers/web/status", 500, 1);
        let ctx = context(&api, NOON);
        let tba = fetch(&api).await;
        let error = reconcile(Arc::clone(&tba), Arc::clone(&ctx)).await.unwrap_err();
        assert!(matches!(error, Error::Kube(_)), "{error}");
        assert_eq!(error_policy(tba, &error, Arc::clone(&ctx)), Action::requeue(RETRY));
        assert!(ctx.metrics.encode().contains("outcome=\"failure\"} 1"));
    }

    #[test]
    fn delay_is_bounded_by_the_resync_interval() {
        let now: Timestamp = NOON.parse().unwrap();
        let minute = Duration::from_secs(60);
        assert_eq!(delay(now, None, minute), minute);
        assert_eq!(delay(now, Some(now + Duration::from_secs(30)), minute), Duration::from_secs(30));
        assert_eq!(delay(now, Some(now + Duration::from_secs(3600)), minute), minute);
        assert_eq!(delay(now, Some(now - Duration::from_secs(1)), minute), minute);
    }

    #[test]
    fn system_clock_is_current() {
        assert!(Clock::System.now() > NOON.parse().unwrap());
    }
}
