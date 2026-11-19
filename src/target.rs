//! Resolution of scale targets and enforcement of the floor on them.
//!
//! Three kinds of targets are supported:
//!
//! * `HorizontalPodAutoscaler`: the floor is written to `spec.minReplicas`; the
//!   HPA keeps scaling above it on CPU, memory or custom metrics.
//! * KEDA `ScaledObject`: the floor is written to `spec.minReplicaCount`.
//! * Anything exposing the `scale` subresource: replicas are raised to the
//!   floor and only lowered again when the controller raised them itself.

use std::collections::HashMap;
use std::sync::Mutex;

use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::discovery::pinned_kind;
use kube::{Client, Error as KubeError};
use serde_json::json;

use crate::crd::{CONTROLLER_NAME, ScaleTargetRef};

/// How the floor is written to a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Raise `spec.replicas` through the `scale` subresource.
    Scale,
    /// Manage a minimum-replicas field of an autoscaler.
    Field(FloorField),
}

/// A minimum-replicas field on an autoscaler object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorField {
    /// Field under `spec` holding the minimum.
    pub min: &'static str,
    /// Field under `spec` holding the maximum.
    pub max: &'static str,
    /// Value of the minimum when unset.
    pub default_min: i32,
    /// Value of the maximum when unset.
    pub default_max: Option<i32>,
    /// Smallest minimum the autoscaler accepts.
    pub lower_bound: i32,
}

/// `autoscaling/v2` `HorizontalPodAutoscaler`.
pub const HPA: FloorField =
    FloorField { min: "minReplicas", max: "maxReplicas", default_min: 1, default_max: None, lower_bound: 1 };

/// KEDA `ScaledObject`.
pub const SCALED_OBJECT: FloorField = FloorField {
    min: "minReplicaCount",
    max: "maxReplicaCount",
    default_min: 0,
    default_max: Some(100),
    lower_bound: 0,
};

/// A resolved target type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// API resource used to build requests.
    pub resource: ApiResource,
    /// How the floor is written.
    pub mode: Mode,
}

/// Why the floor could not be enforced.
#[derive(Debug, thiserror::Error)]
pub enum TargetError {
    /// The API server does not serve the referenced kind.
    #[error("{0} is not served by the API server")]
    UnknownKind(String),
    /// The referenced kind has no `scale` subresource.
    #[error("{0} does not expose the scale subresource")]
    NotScalable(String),
    /// The referenced object does not exist.
    #[error("{kind} {name:?} not found")]
    NotFound {
        /// Kind of the target.
        kind: String,
        /// Name of the target.
        name: String,
    },
    /// Reading the target failed.
    #[error("reading {kind} {name:?} failed: {source}")]
    Read {
        /// Kind of the target.
        kind: String,
        /// Name of the target.
        name: String,
        /// Underlying API error.
        source: KubeError,
    },
    /// Writing the target failed.
    #[error("updating {kind} {name:?} failed: {source}")]
    Write {
        /// Kind of the target.
        kind: String,
        /// Name of the target.
        name: String,
        /// Underlying API error.
        source: KubeError,
    },
}

impl TargetError {
    /// `PascalCase` reason used in conditions and events.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::UnknownKind(_) | Self::NotScalable(_) => "UnsupportedTarget",
            Self::NotFound { .. } => "TargetNotFound",
            Self::Read { .. } => "FailedGetTarget",
            Self::Write { .. } => "FailedUpdateTarget",
        }
    }
}

/// Outcome of enforcing the floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Value before and after the write, when a write happened.
    pub change: Option<(i32, i32)>,
    /// Replicas owned by the controller (scale targets only).
    pub managed_replicas: Option<i32>,
    /// Value enforced on the target.
    pub enforced: i32,
    /// Explanation when the floor had to be adjusted to the target's bounds.
    pub clamped: Option<String>,
}

/// Caches discovery results per `apiVersion` and kind.
#[derive(Debug, Default)]
pub struct Resolver {
    cache: Mutex<HashMap<(String, String), Resolved>>,
}

impl Resolver {
    /// Resolves a reference through API discovery, caching successes.
    ///
    /// # Errors
    ///
    /// Fails when the kind is not served or cannot be scaled.
    pub async fn resolve(&self, client: &Client, target: &ScaleTargetRef) -> Result<Resolved, TargetError> {
        let key = (target.api_version.clone(), target.kind.clone());
        if let Some(resolved) = self.lock().get(&key) {
            return Ok(resolved.clone());
        }
        let (group, version) = target.api_version.split_once('/').unwrap_or(("", &target.api_version));
        let gvk = GroupVersionKind::gvk(group, version, &target.kind);
        let describe = || format!("{}/{}", target.api_version, target.kind);
        let (resource, capabilities) =
            pinned_kind(client, &gvk).await.map_err(|_| TargetError::UnknownKind(describe()))?;
        let mode = match (group, target.kind.as_str()) {
            ("autoscaling", "HorizontalPodAutoscaler") => Mode::Field(HPA),
            ("keda.sh", "ScaledObject") => Mode::Field(SCALED_OBJECT),
            _ if capabilities.subresources.iter().any(|(sub, _)| sub.plural == "scale") => Mode::Scale,
            _ => return Err(TargetError::NotScalable(describe())),
        };
        let resolved = Resolved { resource, mode };
        self.lock().insert(key, resolved.clone());
        Ok(resolved)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), Resolved>> {
        self.cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Decides the replicas of a `scale` target.
///
/// Returns the replicas to write (if any) and the replicas the controller owns
/// afterwards. Replicas are raised to the floor; they are only lowered when the
/// controller set the current value itself, so scale-ups by other actors stick.
#[must_use]
pub fn plan_scale(current: i32, floor: i32, managed: Option<i32>) -> (Option<i32>, Option<i32>) {
    if current < floor || (current > floor && managed == Some(current)) {
        (Some(floor), Some(floor))
    } else {
        (None, managed.filter(|&owned| owned == current))
    }
}

/// Decides the minimum of an autoscaler, clamped to its accepted range.
#[must_use]
pub fn plan_field(field: FloorField, floor: i32, max: Option<i32>) -> (i32, Option<String>) {
    let max = max.or(field.default_max);
    if let Some(max) = max.filter(|&max| floor > max) {
        return (max, Some(format!("floor {floor} exceeds {} {max}", field.max)));
    }
    if floor < field.lower_bound {
        return (
            field.lower_bound,
            Some(format!("floor {floor} raised to the minimum supported {} {}", field.min, field.lower_bound)),
        );
    }
    (floor, None)
}

/// Enforces `floor` on the target.
///
/// # Errors
///
/// Fails when the target cannot be read or written.
pub async fn apply(
    client: &Client,
    namespace: &str,
    target: &ScaleTargetRef,
    resolved: &Resolved,
    floor: i32,
    managed: Option<i32>,
) -> Result<Applied, TargetError> {
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resolved.resource);
    let name = target.name.as_str();
    let read_error = |source: KubeError| match source {
        KubeError::Api(status) if status.is_not_found() => {
            TargetError::NotFound { kind: target.kind.clone(), name: name.into() }
        }
        source => TargetError::Read { kind: target.kind.clone(), name: name.into(), source },
    };
    let write_error = |source| TargetError::Write { kind: target.kind.clone(), name: name.into(), source };
    let params = PatchParams { field_manager: Some(CONTROLLER_NAME.into()), ..PatchParams::default() };

    match resolved.mode {
        Mode::Scale => {
            let scale = api.get_scale(name).await.map_err(read_error)?;
            let current = scale.spec.and_then(|spec| spec.replicas).unwrap_or(0);
            let (write, managed_replicas) = plan_scale(current, floor, managed);
            if let Some(replicas) = write {
                let patch = Patch::Merge(json!({ "spec": { "replicas": replicas } }));
                api.patch_scale(name, &params, &patch).await.map_err(write_error)?;
            }
            Ok(Applied {
                change: write.map(|replicas| (current, replicas)),
                managed_replicas,
                enforced: floor,
                clamped: None,
            })
        }
        Mode::Field(field) => {
            let object = api.get(name).await.map_err(read_error)?;
            let spec = &object.data["spec"];
            let read = |key: &str| spec[key].as_i64().and_then(|value| i32::try_from(value).ok());
            let current = read(field.min).unwrap_or(field.default_min);
            let (desired, clamped) = plan_field(field, floor, read(field.max));
            if desired != current {
                let patch = Patch::Merge(json!({ "spec": { field.min: desired } }));
                api.patch(name, &params, &patch).await.map_err(write_error)?;
            }
            Ok(Applied {
                change: (desired != current).then_some((current, desired)),
                managed_replicas: None,
                enforced: desired,
                clamped,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_plan_raises_and_releases_only_owned_replicas() {
        assert_eq!(plan_scale(1, 5, None), (Some(5), Some(5)));
        assert_eq!(plan_scale(5, 2, Some(5)), (Some(2), Some(2)));
        assert_eq!(plan_scale(8, 2, Some(5)), (None, None));
        assert_eq!(plan_scale(5, 5, Some(5)), (None, Some(5)));
        assert_eq!(plan_scale(5, 5, None), (None, None));
    }

    #[test]
    fn field_plan_respects_bounds() {
        assert_eq!(plan_field(HPA, 4, Some(10)), (4, None));
        assert_eq!(plan_field(HPA, 0, Some(10)).0, 1);
        let (value, message) = plan_field(HPA, 20, Some(10));
        assert_eq!(value, 10);
        assert_eq!(message.unwrap(), "floor 20 exceeds maxReplicas 10");
        assert_eq!(plan_field(SCALED_OBJECT, 0, None), (0, None));
        assert_eq!(plan_field(SCALED_OBJECT, 200, None).0, 100);
    }

    #[test]
    fn reasons_are_pascal_case() {
        let kube = || KubeError::LinesCodecMaxLineLengthExceeded;
        let errors = [
            TargetError::UnknownKind("x".into()),
            TargetError::NotScalable("x".into()),
            TargetError::NotFound { kind: "Deployment".into(), name: "web".into() },
            TargetError::Read { kind: "Deployment".into(), name: "web".into(), source: kube() },
            TargetError::Write { kind: "Deployment".into(), name: "web".into(), source: kube() },
        ];
        let reasons: Vec<_> = errors.iter().map(TargetError::reason).collect();
        assert_eq!(
            reasons,
            ["UnsupportedTarget", "UnsupportedTarget", "TargetNotFound", "FailedGetTarget", "FailedUpdateTarget"]
        );
        assert_eq!(errors[2].to_string(), r#"Deployment "web" not found"#);
    }
}
