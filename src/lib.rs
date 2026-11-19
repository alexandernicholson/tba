//! `tba`: a Kubernetes time based autoscaler.
//!
//! A `TimeBasedAutoscaler` object declares weekly time windows with replica
//! floors. The controller enforces the floor that applies now on a scale
//! target, leaving `HorizontalPodAutoscaler` or KEDA free to scale above it.

pub mod cli;
pub mod controller;
pub mod crd;
pub mod error;
pub mod evaluate;
pub mod metrics;
pub mod reconcile;
pub mod schedule;
pub mod server;
pub mod target;

#[cfg(test)]
#[path = "../tests/support/fake_api.rs"]
pub(crate) mod fake_api;
