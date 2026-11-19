//! Controller wiring: watch, reconcile loop and leader election.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use jiff::tz::TimeZone;
use kube::Client;
use kube::api::Api;
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::{Controller, PredicateConfig, WatchStreamExt, predicates, reflector, watcher};
use kube_lease_manager::LeaseManagerBuilder;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::crd::{CONTROLLER_NAME, TimeBasedAutoscaler};
use crate::error::Error;
use crate::metrics::Metrics;
use crate::reconcile::{Clock, Context, error_policy, reconcile};
use crate::target::Resolver;

/// Lease validity; matches the controller-runtime default.
const LEASE_DURATION_SECONDS: u64 = 15;
/// How long before expiry the leader renews; leaves 10s to renew like controller-runtime.
const LEASE_GRACE_SECONDS: u64 = 5;

/// Leader election settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElection {
    /// Name of the `coordination.k8s.io/v1` `Lease`.
    pub lease_name: String,
    /// Namespace of the lease.
    pub lease_namespace: String,
}

/// Controller settings.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Time zone schedules are evaluated in.
    pub tz: TimeZone,
    /// Namespace to watch; all namespaces when `None`.
    pub namespace: Option<String>,
    /// Maximum delay between two reconciliations of an object.
    pub resync: Duration,
    /// Delay before retrying a failed reconciliation.
    pub retry: Duration,
    /// Leader election, disabled when `None`.
    pub leader_election: Option<LeaderElection>,
    /// Identity of this replica in leases and events.
    pub identity: String,
    /// Time source.
    pub clock: Clock,
}

/// Runs the controller until `shutdown` is cancelled.
///
/// # Errors
///
/// Fails when leader election fails or leadership is lost; the process should
/// then exit so that a fresh replica can take over.
pub async fn run(
    client: Client,
    settings: Settings,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let Some(election) = settings.leader_election.clone() else {
        run_controller(client, settings, metrics, shutdown).await;
        return Ok(());
    };
    let manager = LeaseManagerBuilder::new(client.clone(), &election.lease_name)
        .with_namespace(&election.lease_namespace)
        .with_identity(&settings.identity)
        .with_duration(LEASE_DURATION_SECONDS)
        .with_grace(LEASE_GRACE_SECONDS)
        .with_field_manager(CONTROLLER_NAME)
        .build()
        .await?;
    let (mut leading, task) = manager.watch().await;
    tracing::info!(lease = election.lease_name, identity = settings.identity, "waiting for leadership");
    let result = if wait_for(&mut leading, true, &shutdown).await {
        tracing::info!("acquired leadership");
        metrics.leader.set(1);
        let controller = run_controller(client, settings, Arc::clone(&metrics), shutdown.clone());
        tokio::pin!(controller);
        let result = tokio::select! {
            () = &mut controller => Ok(()),
            lost = wait_for(&mut leading, false, &shutdown) => if lost {
                tracing::error!("leadership lost");
                Err(Error::LeadershipLost)
            } else {
                // Shutdown requested: let in-flight reconciliations finish.
                controller.await;
                Ok(())
            },
        };
        metrics.leader.set(0);
        result
    } else {
        Ok(())
    };
    // Closing the channel makes the lease manager release the lease.
    drop(leading);
    task.await??;
    result
}

/// Waits until the leadership flag equals `wanted`. Returns `false` when
/// shutdown was requested first. A stopped lease manager counts as not leading.
async fn wait_for(leading: &mut watch::Receiver<bool>, wanted: bool, shutdown: &CancellationToken) -> bool {
    loop {
        if *leading.borrow_and_update() == wanted {
            return true;
        }
        tokio::select! {
            changed = leading.changed() => if changed.is_err() { return !wanted },
            () = shutdown.cancelled() => return false,
        }
    }
}

async fn run_controller(client: Client, settings: Settings, metrics: Arc<Metrics>, shutdown: CancellationToken) {
    let api: Api<TimeBasedAutoscaler> = settings
        .namespace
        .as_deref()
        .map_or_else(|| Api::all(client.clone()), |namespace| Api::namespaced(client.clone(), namespace));
    let recorder = Recorder::new(
        client.clone(),
        Reporter { controller: CONTROLLER_NAME.into(), instance: Some(settings.identity.clone()) },
    );
    let context = Arc::new(Context {
        client,
        tz: settings.tz,
        resync: settings.resync,
        retry: settings.retry,
        clock: settings.clock,
        recorder,
        resolver: Resolver::default(),
        metrics,
    });

    // Status writes do not change `metadata.generation`; filtering on it keeps
    // the controller from reconciling its own status updates.
    let (reader, writer) = reflector::store();
    let stream = watcher(api, watcher::Config::default().any_semantic())
        .default_backoff()
        .reflect(writer)
        .applied_objects()
        .predicate_filter(predicates::generation, PredicateConfig::default());

    tracing::info!("starting controller");
    Controller::for_stream(stream, reader)
        .graceful_shutdown_on(shutdown.cancelled_owned())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((object, _)) => tracing::debug!(namespace = object.namespace, name = object.name, "reconciled"),
                Err(error) => tracing::debug!(%error, "reconciliation did not complete"),
            }
        })
        .await;
    tracing::info!("controller stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_api::FakeApi;
    use serde_json::{Value, json};

    const TBA: &str = "/apis/tba.alexandernicholson.github.io/v1alpha1/namespaces/default/timebasedautoscalers/web";
    const TBA_COLLECTION: &str =
        "GET /apis/tba.alexandernicholson.github.io/v1alpha1/namespaces/default/timebasedautoscalers";
    const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/default/deployments/web";
    const LEASE: &str = "/apis/coordination.k8s.io/v1/namespaces/default/leases/tba";

    fn settings(namespace: Option<&str>, leader_election: bool) -> Settings {
        Settings {
            tz: TimeZone::UTC,
            namespace: namespace.map(str::to_owned),
            resync: Duration::from_secs(60),
            retry: Duration::from_secs(1),
            leader_election: leader_election
                .then(|| LeaderElection { lease_name: "tba".into(), lease_namespace: "default".into() }),
            identity: "tba-0".into(),
            clock: Clock::System,
        }
    }

    /// Stores a TBA whose single always-on window enforces `floor`.
    fn insert_tba(api: &FakeApi, floor: i32, generation: i64) {
        api.insert(
            TBA,
            json!({
                "apiVersion": "tba.alexandernicholson.github.io/v1alpha1", "kind": "TimeBasedAutoscaler",
                "metadata": {"name": "web", "namespace": "default", "generation": generation},
                "spec": {
                    "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                    "minReplicas": 0,
                    "schedules": [{"name": "always", "start": "00:00", "end": "24:00", "minReplicas": floor}]
                }
            }),
        );
    }

    async fn setup() -> FakeApi {
        let api = FakeApi::start().await;
        insert_tba(&api, 3, 1);
        api.insert(DEPLOYMENT, json!({"metadata": {"name": "web", "namespace": "default"}, "spec": {"replicas": 1}}));
        api
    }

    async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
        let mut satisfied = condition();
        for _ in 0..400 {
            if satisfied {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            satisfied = condition();
        }
        assert!(satisfied, "timed out waiting for {what}");
    }

    fn replicas(api: &FakeApi) -> Value {
        api.get(DEPLOYMENT).unwrap()["spec"]["replicas"].clone()
    }

    fn holder(api: &FakeApi) -> Value {
        api.get(LEASE).map_or(Value::Null, |lease| lease["spec"]["holderIdentity"].clone())
    }

    fn foreign_lease(api: &FakeApi) {
        api.insert(
            LEASE,
            json!({
                "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                "metadata": {"name": "tba", "namespace": "default"},
                "spec": {"holderIdentity": "other", "leaseDurationSeconds": 3600, "renewTime": jiff::Timestamp::now().to_string()}
            }),
        );
    }

    #[tokio::test]
    async fn reconciles_spec_changes_until_shutdown() {
        let api = setup().await;
        // The first status write fails: the reconciliation is retried.
        api.fail("PATCH", "/timebasedautoscalers/web/status", 500, 1);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(api.client(), settings(Some("default"), false), Arc::default(), shutdown.clone()));
        wait_until("floor applied", || replicas(&api) == 3).await;
        wait_until("watch started", || api.count(TBA_COLLECTION) >= 2).await;
        insert_tba(&api, 4, 2);
        wait_until("new floor applied", || replicas(&api) == 4).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn leader_reconciles_and_releases_the_lease_on_shutdown() {
        let api = setup().await;
        let metrics = Arc::new(Metrics::default());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(api.client(), settings(None, true), Arc::clone(&metrics), shutdown.clone()));
        wait_until("floor applied", || replicas(&api) == 3).await;
        assert_eq!(holder(&api), "tba-0");
        assert_eq!(metrics.leader.get(), 1);
        shutdown.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(holder(&api), Value::Null);
        assert_eq!(metrics.leader.get(), 0);
    }

    #[tokio::test]
    async fn standby_does_not_reconcile() {
        let api = setup().await;
        foreign_lease(&api);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(api.client(), settings(None, true), Arc::default(), shutdown.clone()));
        wait_until("lease read", || api.count(&format!("GET {LEASE}")) >= 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(replicas(&api), 1);
        assert_eq!(holder(&api), "other");
    }

    #[tokio::test]
    async fn losing_the_lease_stops_the_controller() {
        let api = setup().await;
        let task = tokio::spawn(run(api.client(), settings(None, true), Arc::default(), CancellationToken::new()));
        wait_until("floor applied", || replicas(&api) == 3).await;
        foreign_lease(&api);
        let result = tokio::time::timeout(Duration::from_secs(30), task).await.unwrap().unwrap();
        assert!(matches!(result, Err(Error::LeadershipLost)), "{result:?}");
    }

    #[tokio::test]
    async fn lease_errors_fail_startup() {
        let api = setup().await;
        api.fail("GET", "/leases/tba", 403, 1);
        let result = run(api.client(), settings(None, true), Arc::default(), CancellationToken::new()).await;
        assert!(matches!(result, Err(Error::LeaderElection(_))), "{result:?}");
    }
}
