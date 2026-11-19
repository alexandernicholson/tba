//! HTTP endpoint serving `/healthz`, `/readyz` and `/metrics`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;

const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
/// Pause after a failed `accept` so descriptor exhaustion does not spin the loop.
const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// State shared between the controller and the HTTP endpoint.
#[derive(Debug, Default)]
pub struct Health {
    /// Metrics rendered on `/metrics`.
    pub metrics: Arc<Metrics>,
    shutting_down: AtomicBool,
}

impl Health {
    /// Creates the state with the given metrics.
    #[must_use]
    pub const fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics, shutting_down: AtomicBool::new(false) }
    }

    /// Marks the process as terminating so `/readyz` fails.
    pub fn shutting_down(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
    }

    fn respond(&self, request: &Request<Incoming>) -> Response<Full<Bytes>> {
        let (status, content_type, body) = match (request.method(), request.uri().path()) {
            (&Method::GET | &Method::HEAD, "/readyz") if self.shutting_down.load(Ordering::Relaxed) => {
                (StatusCode::SERVICE_UNAVAILABLE, "text/plain", Bytes::from_static(b"shutting down"))
            }
            (&Method::GET | &Method::HEAD, "/healthz" | "/livez" | "/readyz") => {
                (StatusCode::OK, "text/plain", Bytes::from_static(b"ok"))
            }
            (&Method::GET | &Method::HEAD, "/metrics") => {
                (StatusCode::OK, OPENMETRICS, Bytes::from(self.metrics.encode()))
            }
            _ => (StatusCode::NOT_FOUND, "text/plain", Bytes::from_static(b"not found")),
        };
        let mut response = Response::new(Full::new(body));
        *response.status_mut() = status;
        response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        response
    }
}

/// Binds the listener and returns its address with the serving future.
///
/// The future completes once `shutdown` is cancelled.
///
/// # Errors
///
/// Fails when the address cannot be bound.
pub async fn bind(
    addr: SocketAddr,
    health: Arc<Health>,
    shutdown: CancellationToken,
) -> std::io::Result<(SocketAddr, impl Future<Output = ()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    Ok((local, serve(listener, health, shutdown)))
}

async fn serve(listener: TcpListener, health: Arc<Health>, shutdown: CancellationToken) {
    loop {
        let accepted = tokio::select! {
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(error) => {
                tracing::warn!(%error, "accepting HTTP connection failed");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let health = Arc::clone(&health);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let response = health.respond(&request);
                async move { Ok::<_, Infallible>(response) }
            });
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn get(addr: SocketAddr, method: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("{method} {path} HTTP/1.1\r\nHost: tba\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn serves_probes_and_metrics_until_shutdown() {
        let health = Arc::new(Health::new(Arc::default()));
        let shutdown = CancellationToken::new();
        let (addr, serving) =
            bind("127.0.0.1:0".parse().unwrap(), Arc::clone(&health), shutdown.clone()).await.unwrap();
        let task = tokio::spawn(serving);

        assert!(get(addr, "GET", "/healthz").await.starts_with("HTTP/1.1 200 OK"));
        assert!(get(addr, "HEAD", "/livez").await.starts_with("HTTP/1.1 200 OK"));
        assert!(get(addr, "GET", "/readyz").await.ends_with("ok"));
        let metrics = get(addr, "GET", "/metrics").await;
        assert!(metrics.contains("content-type: application/openmetrics-text"), "{metrics}");
        assert!(metrics.contains("# TYPE tba_leader gauge"), "{metrics}");
        assert!(get(addr, "GET", "/missing").await.starts_with("HTTP/1.1 404"));
        assert!(get(addr, "POST", "/healthz").await.starts_with("HTTP/1.1 404"));

        health.shutting_down();
        assert!(get(addr, "GET", "/readyz").await.starts_with("HTTP/1.1 503"));
        assert!(get(addr, "GET", "/healthz").await.starts_with("HTTP/1.1 200"));

        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn binding_a_taken_address_fails() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap();
        let health = Arc::new(Health::new(Arc::default()));
        assert!(bind(addr, health, CancellationToken::new()).await.is_err());
    }
}
