//! Crate-wide error type.

use thiserror::Error;

/// Errors surfaced by the controller and the command line.
#[derive(Debug, Error)]
pub enum Error {
    /// A Kubernetes API request failed.
    #[error("kubernetes API request failed: {0}")]
    Kube(#[from] kube::Error),
    /// Loading the Kubernetes client configuration failed.
    #[error("loading kubernetes configuration failed: {0}")]
    KubeConfig(#[from] kube::config::InferConfigError),
    /// Leader election failed.
    #[error("leader election failed: {0}")]
    LeaderElection(#[from] kube_lease_manager::LeaseManagerError),
    /// Another replica took over the lease.
    #[error("leadership lost")]
    LeadershipLost,
    /// Reading input or binding a socket failed.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// Input documents could not be decoded.
    #[error("decoding input failed: {0}")]
    Decode(String),
    /// A background task panicked or was cancelled.
    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
