//! End-to-end tests of the `tba` binary against the fake API server.

#[path = "support/fake_api.rs"]
mod fake_api;

use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::Duration;

use fake_api::FakeApi;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const TBA: &str = "/apis/tba.alexandernicholson.github.io/v1alpha1/namespaces/default/timebasedautoscalers/web";
const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/default/deployments/web";
const LEASE: &str = "/apis/coordination.k8s.io/v1/namespaces/default/leases/tba-controller";

async fn setup(name: &str) -> (FakeApi, PathBuf) {
    let api = FakeApi::start().await;
    api.insert(
        TBA,
        json!({
            "apiVersion": "tba.alexandernicholson.github.io/v1alpha1", "kind": "TimeBasedAutoscaler",
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {
                "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                "minReplicas": 3
            }
        }),
    );
    api.insert(DEPLOYMENT, json!({"metadata": {"name": "web", "namespace": "default"}, "spec": {"replicas": 1}}));
    let dir = std::env::temp_dir().join(format!("tba-it-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kubeconfig = api.kubeconfig(&dir);
    (api, kubeconfig)
}

fn tba(kubeconfig: &PathBuf) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tba"));
    for name in ["POD_NAME", "POD_NAMESPACE", "HOSTNAME", "TBA_TIMEZONE", "TBA_WATCH_NAMESPACE", "TBA_LEADER_ELECT"] {
        command.env_remove(name);
    }
    command.env("KUBECONFIG", kubeconfig).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    command
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn http_get(port: u16, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: tba\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    Some(response)
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..400 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn signal(child: &Child, name: &str) {
    let pid = child.id().unwrap().to_string();
    let status = Command::new("kill").args([format!("-{name}").as_str(), pid.as_str()]).status().await.unwrap();
    assert!(status.success());
}

async fn finish(child: Child) -> Output {
    tokio::time::timeout(Duration::from_secs(30), child.wait_with_output()).await.unwrap().unwrap()
}

fn replicas(api: &FakeApi) -> Value {
    api.get(DEPLOYMENT).unwrap()["spec"]["replicas"].clone()
}

#[tokio::test]
async fn enforces_the_floor_and_terminates_gracefully_on_sigterm() {
    let (api, kubeconfig) = setup("sigterm").await;
    let port = free_port();
    let child = tba(&kubeconfig)
        .args([
            "run",
            "--http-addr",
            &format!("127.0.0.1:{port}"),
            "--timezone",
            "Europe/Berlin",
            "--log-format",
            "text",
        ])
        .spawn()
        .unwrap();
    eventually("floor applied", async || replicas(&api) == 3).await;
    eventually("ready", async || http_get(port, "/readyz").await.is_some_and(|r| r.starts_with("HTTP/1.1 200"))).await;
    let metrics = http_get(port, "/metrics").await.unwrap();
    assert!(metrics.contains("tba_leader 1"), "{metrics}");
    assert_eq!(api.get(LEASE).unwrap()["spec"]["holderIdentity"], format!("tba-controller-{}", child.id().unwrap()));

    signal(&child, "TERM").await;
    let output = finish(child).await;
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{logs}");
    assert!(logs.contains("shutdown requested"), "{logs}");
    assert_eq!(api.get(LEASE).unwrap()["spec"]["holderIdentity"], Value::Null, "lease released");
}

#[tokio::test]
async fn sigint_stops_a_namespaced_controller_without_leader_election() {
    let (api, kubeconfig) = setup("sigint").await;
    let child = tba(&kubeconfig)
        .args(["run", "--http-addr", "127.0.0.1:0", "--leader-elect=false", "--namespace", "default"])
        .env("POD_NAME", "tba-test")
        .spawn()
        .unwrap();
    eventually("floor applied", async || replicas(&api) == 3).await;
    signal(&child, "INT").await;
    let output = finish(child).await;
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{logs}");
    assert!(logs.contains(r#""identity":"tba-test""#), "{logs}");
    assert!(api.get(LEASE).is_none());
}

#[tokio::test]
async fn startup_failures_exit_non_zero() {
    let (api, kubeconfig) = setup("failures").await;

    let output = finish(tba(&PathBuf::from("/nonexistent/kubeconfig")).arg("run").spawn().unwrap()).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("loading kubernetes configuration failed"));

    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap().to_string();
    let output = finish(tba(&kubeconfig).args(["run", "--http-addr", &addr]).spawn().unwrap()).await;
    assert_eq!(output.status.code(), Some(1));

    api.fail("GET", "/leases/tba-controller", 403, 1);
    let output = finish(tba(&kubeconfig).args(["run", "--http-addr", "127.0.0.1:0"]).spawn().unwrap()).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("error: leader election failed"));
}

#[tokio::test]
async fn prints_version_and_crd() {
    let output = finish(tba(&PathBuf::new()).arg("--version").spawn().unwrap()).await;
    assert_eq!(String::from_utf8_lossy(&output.stdout), format!("tba {}\n", env!("CARGO_PKG_VERSION")));
    let output = finish(tba(&PathBuf::new()).arg("crd").spawn().unwrap()).await;
    assert!(String::from_utf8_lossy(&output.stdout).contains("kind: CustomResourceDefinition"));
}

#[tokio::test]
async fn health_endpoint_survives_descriptor_exhaustion() {
    let (api, kubeconfig) = setup("emfile").await;
    let port = free_port();
    let child = Command::new("sh")
        .args(["-c", r#"ulimit -n 48 && exec "$0" "$@""#, env!("CARGO_BIN_EXE_tba")])
        .args(["run", "--http-addr", &format!("127.0.0.1:{port}"), "--leader-elect=false"])
        .env("KUBECONFIG", &kubeconfig)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    eventually("floor applied", async || replicas(&api) == 3).await;

    // Idle keep-alive connections pin one descriptor each until accept fails.
    let mut idle = Vec::new();
    for _ in 0..64 {
        idle.push(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(idle);
    eventually("recovered", async || http_get(port, "/healthz").await.is_some_and(|r| r.starts_with("HTTP/1.1 200")))
        .await;

    signal(&child, "TERM").await;
    let output = finish(child).await;
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{logs}");
    assert!(logs.contains("accepting HTTP connection failed"), "{logs}");
}

#[tokio::test]
async fn evaluates_manifests_from_stdin_and_files() {
    let manifest = "kind: TimeBasedAutoscaler\nmetadata: {name: web}\nspec:\n  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}\n  minReplicas: 2\n";
    let mut child = tba(&PathBuf::new()).args(["evaluate", "-f", "-"]).stdin(Stdio::piped()).spawn().unwrap();
    child.stdin.take().unwrap().write_all(manifest.as_bytes()).await.unwrap();
    let output = finish(child).await;
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "NAMESPACE\tNAME\tFLOOR\tACTIVE\tNEXT\ndefault\tweb\t2\t-\t-\n"
    );

    let path = std::env::temp_dir().join(format!("tba-it-{}-evaluate.yaml", std::process::id()));
    std::fs::write(&path, manifest).unwrap();
    let output = finish(tba(&PathBuf::new()).args(["evaluate", "-f"]).arg(&path).spawn().unwrap()).await;
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).ends_with("default\tweb\t2\t-\t-\n"));
}
