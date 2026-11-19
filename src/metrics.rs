//! Prometheus metrics exposed on `/metrics`.

use std::fmt::Write as _;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// Outcome of a reconciliation.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Outcome {
    /// The floor is enforced.
    Success,
    /// The floor could not be enforced.
    Failure,
}

impl EncodeLabelValue for Outcome {
    fn encode(&self, encoder: &mut LabelValueEncoder<'_>) -> std::fmt::Result {
        encoder.write_str(match self {
            Self::Success => "success",
            Self::Failure => "failure",
        })
    }
}

/// Labels of `tba_reconcile_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabels {
    /// Outcome of the reconciliation.
    pub outcome: Outcome,
}

/// Direction of a target update.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Direction {
    /// Replicas or floor raised.
    Up,
    /// Replicas or floor lowered.
    Down,
}

impl EncodeLabelValue for Direction {
    fn encode(&self, encoder: &mut LabelValueEncoder<'_>) -> std::fmt::Result {
        encoder.write_str(match self {
            Self::Up => "up",
            Self::Down => "down",
        })
    }
}

/// Labels of `tba_target_updates_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectionLabels {
    /// Direction of the update.
    pub direction: Direction,
}

/// Controller metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    /// Reconciliations by outcome.
    pub reconciles: Family<OutcomeLabels, Counter>,
    /// Reconciliation latency.
    pub reconcile_duration: Histogram,
    /// Writes to scale targets by direction.
    pub target_updates: Family<DirectionLabels, Counter>,
    /// `1` while this replica holds the leader lease.
    pub leader: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        let mut registry = Registry::with_prefix("tba");
        let reconciles = Family::<OutcomeLabels, Counter>::default();
        registry.register("reconcile", "Reconciliations by outcome", reconciles.clone());
        let reconcile_duration = Histogram::new(exponential_buckets(0.001, 2.0, 14));
        registry.register("reconcile_duration_seconds", "Reconciliation latency", reconcile_duration.clone());
        let target_updates = Family::<DirectionLabels, Counter>::default();
        registry.register("target_updates", "Writes to scale targets by direction", target_updates.clone());
        let leader = Gauge::default();
        registry.register("leader", "1 while this replica holds the leader lease", leader.clone());
        Self { registry, reconciles, reconcile_duration, target_updates, leader }
    }
}

impl Metrics {
    /// Encodes all metrics in the `OpenMetrics` text format.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut buffer = String::with_capacity(2048);
        // Writing to a String cannot fail.
        let _ = prometheus_client::encoding::text::encode(&mut buffer, &self.registry);
        buffer
    }

    /// Records a reconciliation outcome.
    pub fn reconciled(&self, outcome: Outcome) {
        self.reconciles.get_or_create(&OutcomeLabels { outcome }).inc();
    }

    /// Records a write to a scale target.
    pub fn target_updated(&self, direction: Direction) {
        self.target_updates.get_or_create(&DirectionLabels { direction }).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_registered_metrics() {
        let metrics = Metrics::default();
        metrics.reconciled(Outcome::Success);
        metrics.reconciled(Outcome::Failure);
        metrics.target_updated(Direction::Up);
        metrics.target_updated(Direction::Down);
        metrics.reconcile_duration.observe(0.002);
        metrics.leader.set(1);
        let text = metrics.encode();
        assert!(text.contains(r#"tba_reconcile_total{outcome="success"} 1"#), "{text}");
        assert!(text.contains(r#"tba_reconcile_total{outcome="failure"} 1"#), "{text}");
        assert!(text.contains(r#"tba_target_updates_total{direction="up"} 1"#), "{text}");
        assert!(text.contains(r#"tba_target_updates_total{direction="down"} 1"#), "{text}");
        assert!(text.contains("tba_reconcile_duration_seconds_count 1"), "{text}");
        assert!(text.contains("tba_leader 1"), "{text}");
        assert!(text.ends_with("# EOF\n"));
    }
}
