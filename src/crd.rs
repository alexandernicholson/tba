//! The `TimeBasedAutoscaler` custom resource definition.
//!
//! The API follows the Kubernetes API conventions: a declarative `spec`, an
//! observed `status` written only through the status subresource,
//! `metav1.Condition` conditions and `observedGeneration`.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::{CustomResource, CustomResourceExt, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// API group of the custom resource.
pub const GROUP: &str = "tba.alexandernicholson.github.io";

/// Name of the controller, used as field manager, event reporter and lease name prefix.
pub const CONTROLLER_NAME: &str = "tba-controller";

/// Condition type reporting whether the floor is currently enforced on the target.
pub const CONDITION_READY: &str = "Ready";

/// `TimeBasedAutoscaler` enforces a time-of-week dependent replica floor on a scale target.
///
/// The floor is a lower bound only: horizontal autoscalers such as the
/// `HorizontalPodAutoscaler` or KEDA remain free to scale above it.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[kube(
    group = "tba.alexandernicholson.github.io",
    version = "v1alpha1",
    kind = "TimeBasedAutoscaler",
    plural = "timebasedautoscalers",
    singular = "timebasedautoscaler",
    shortname = "tba",
    category = "autoscaling",
    namespaced,
    status = "TimeBasedAutoscalerStatus",
    derive = "PartialEq",
    doc = "TimeBasedAutoscaler enforces a time-of-week dependent replica floor on a scale target.",
    printcolumn = r#"{"name":"Kind","type":"string","jsonPath":".spec.scaleTargetRef.kind","description":"Kind of the scale target"}"#,
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.scaleTargetRef.name","description":"Name of the scale target"}"#,
    printcolumn = r#"{"name":"Floor","type":"integer","jsonPath":".status.desiredMinReplicas","description":"Replica floor currently enforced"}"#,
    printcolumn = r#"{"name":"Active","type":"string","jsonPath":".status.activeSchedules","description":"Schedules currently in effect"}"#,
    printcolumn = r#"{"name":"Next","type":"date","jsonPath":".status.nextTransitionTime","description":"Next time the floor changes","priority":1}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status","description":"Whether the floor is enforced"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TimeBasedAutoscalerSpec {
    /// Reference to the scaled resource. Either a resource exposing the `scale`
    /// subresource (`Deployment`, `StatefulSet`, `ReplicaSet`, custom resources), a
    /// `HorizontalPodAutoscaler` (its `minReplicas` is managed) or a KEDA
    /// `ScaledObject` (its `minReplicaCount` is managed).
    pub scale_target_ref: ScaleTargetRef,

    /// Floor applied while no schedule is active.
    #[schemars(range(min = 0))]
    pub min_replicas: i32,

    /// Time windows raising the floor. When several windows overlap, the
    /// highest `minReplicas` wins. Times are evaluated in the controller's
    /// configured time zone (`TBA_TIMEZONE`).
    #[serde(default)]
    #[schemars(length(max = 64))]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["name"]))]
    pub schedules: Vec<Schedule>,
}

/// Reference to the object whose replica floor is managed, in the same namespace.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleTargetRef {
    /// API version of the referent, e.g. `apps/v1`.
    #[schemars(length(min = 1, max = 253))]
    pub api_version: String,
    /// Kind of the referent, e.g. `Deployment`.
    #[schemars(length(min = 1, max = 63))]
    pub kind: String,
    /// Name of the referent.
    #[schemars(length(min = 1, max = 253))]
    pub name: String,
}

/// A recurring weekly time window with its replica floor.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, KubeSchema)]
#[serde(rename_all = "camelCase")]
#[x_kube(validation = kube::core::Rule::new("self.start != self.end").message("start and end must differ"))]
pub struct Schedule {
    /// Unique name of the window, reported in `status.activeSchedules`.
    #[schemars(length(min = 1, max = 63), regex(pattern = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"))]
    pub name: String,

    /// Days on which the window starts. Defaults to every day.
    #[serde(default = "Weekday::all")]
    #[schemars(length(min = 1, max = 7))]
    #[schemars(extend("x-kubernetes-list-type" = "set"))]
    pub days: Vec<Weekday>,

    /// Inclusive local start time, `HH:MM` (24-hour clock).
    #[schemars(length(equal = 5), regex(pattern = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"))]
    pub start: String,

    /// Exclusive local end time, `HH:MM` (24-hour clock) or `24:00`. An end
    /// earlier than the start describes a window crossing midnight.
    #[schemars(length(equal = 5), regex(pattern = r"^(([01][0-9]|2[0-3]):[0-5][0-9]|24:00)$"))]
    pub end: String,

    /// Floor applied while the window is active.
    #[schemars(range(min = 0))]
    pub min_replicas: i32,
}

/// Day of the week.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, JsonSchema)]
pub enum Weekday {
    /// Monday.
    Monday,
    /// Tuesday.
    Tuesday,
    /// Wednesday.
    Wednesday,
    /// Thursday.
    Thursday,
    /// Friday.
    Friday,
    /// Saturday.
    Saturday,
    /// Sunday.
    Sunday,
}

impl Weekday {
    /// Every day of the week, Monday first.
    #[must_use]
    pub fn all() -> Vec<Self> {
        vec![Self::Monday, Self::Tuesday, Self::Wednesday, Self::Thursday, Self::Friday, Self::Saturday, Self::Sunday]
    }

    /// Zero-based index with Monday as `0`.
    #[must_use]
    pub const fn index(self) -> u16 {
        self as u16
    }
}

/// Observed state of a [`TimeBasedAutoscaler`].
///
/// Every field is always serialized (`null` or `[]` when unset) so that a JSON
/// merge patch of the whole status clears fields that no longer apply.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeBasedAutoscalerStatus {
    /// Generation of the spec the status was computed from.
    #[serde(default)]
    pub observed_generation: Option<i64>,

    /// Floor in effect at the last reconciliation.
    #[serde(default)]
    pub desired_min_replicas: Option<i32>,

    /// Names of the schedules in effect at the last reconciliation.
    #[serde(default)]
    pub active_schedules: Vec<String>,

    /// Next time the floor changes. Absent when the floor is constant.
    #[serde(default)]
    pub next_transition_time: Option<Time>,

    /// Replica count last written to a `scale` subresource target. The
    /// controller only lowers replicas it raised itself, so manual or
    /// autoscaler-driven scale-ups above the floor are preserved.
    #[serde(default)]
    pub managed_replicas: Option<i32>,

    /// Last time the controller changed the target.
    #[serde(default)]
    pub last_scale_time: Option<Time>,

    /// Current state of the autoscaler.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<Condition>,
}

/// Renders the `CustomResourceDefinition` manifest as YAML.
///
/// # Panics
///
/// Never in practice: the CRD is a plain serializable value.
#[must_use]
pub fn crd_yaml() -> String {
    serde_saphyr::to_string(&TimeBasedAutoscaler::crd()).expect("CRD serializes to YAML")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crd_carries_conventions() {
        let crd = serde_json::to_value(TimeBasedAutoscaler::crd()).unwrap();
        assert_eq!(crd["metadata"]["name"], "timebasedautoscalers.tba.alexandernicholson.github.io");
        assert_eq!(crd["spec"]["group"], GROUP);
        assert_eq!(crd["spec"]["names"]["shortNames"][0], "tba");
        let version = &crd["spec"]["versions"][0];
        assert_eq!(version["subresources"]["status"], serde_json::json!({}));
        let spec = &version["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let schedules = &spec["properties"]["schedules"];
        assert_eq!(schedules["x-kubernetes-list-type"], "map");
        assert_eq!(schedules["items"]["x-kubernetes-validations"][0]["rule"], "self.start != self.end");
        // CEL cost estimation needs bounded strings and lists, or the API server rejects the CRD.
        assert_eq!(schedules["maxItems"], 64);
        assert_eq!(schedules["items"]["properties"]["start"]["maxLength"], 5);
        assert_eq!(schedules["items"]["properties"]["end"]["maxLength"], 5);
        assert_eq!(schedules["items"]["properties"]["days"]["default"][6], "Sunday");
        let conditions = &version["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"]["conditions"];
        assert_eq!(conditions["x-kubernetes-list-map-keys"][0], "type");
    }

    #[test]
    fn crd_yaml_round_trips() {
        let yaml = crd_yaml();
        assert!(yaml.contains("kind: CustomResourceDefinition"));
        let parsed: serde_json::Value = serde_saphyr::from_str(&yaml).unwrap();
        assert_eq!(parsed["spec"]["names"]["kind"], "TimeBasedAutoscaler");
    }

    #[test]
    fn days_default_to_every_day() {
        let schedule: Schedule =
            serde_json::from_str(r#"{"name":"a","start":"08:00","end":"09:00","minReplicas":1}"#).unwrap();
        assert_eq!(schedule.days.len(), 7);
        assert_eq!(Weekday::Sunday.index(), 6);
    }
}
