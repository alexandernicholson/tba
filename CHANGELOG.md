# Changelog

## Unreleased

- `TimeBasedAutoscaler` API (`tba.alexandernicholson.github.io/v1alpha1`) with
  weekly windows, CEL-validated, status conditions and printer columns.
- Controller enforcing replica floors on `HorizontalPodAutoscaler`
  (`minReplicas`), KEDA `ScaledObject` (`minReplicaCount`) and any resource
  with a `scale` subresource, leaving scale-ups by other actors untouched.
- Schedules evaluated in `TBA_TIMEZONE` with exact handling of DST gaps and folds;
  wake-ups at the next floor change instead of polling.
- Lease-based leader election, graceful shutdown, health probes, OpenMetrics
  metrics and Kubernetes Events.
- `tba evaluate` for offline schedule evaluation (YAML and JSON input) and
  `tba crd` to print the CRD.
- Kustomize deployment manifests following the restricted Pod Security standard.
