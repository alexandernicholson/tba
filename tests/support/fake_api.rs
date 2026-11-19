//! In-memory fake of the Kubernetes API server for tests.
//!
//! It serves discovery, CRUD with JSON merge semantics for every patch type,
//! the `scale` and `status` subresources, lists and watches over plain HTTP,
//! and can inject failures. Shared by unit tests (`src/lib.rs`) and the
//! binary integration tests.
// Test fixture: pedantic/nursery style lints add nothing here.
#![allow(dead_code, clippy::pedantic, clippy::nursery)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

type Body = UnsyncBoxBody<Bytes, Infallible>;

#[derive(Default)]
struct State {
    objects: BTreeMap<String, Value>,
    discovery: BTreeMap<String, Value>,
    failures: Vec<Failure>,
    requests: Vec<String>,
    version: u64,
}

struct Failure {
    method: Method,
    path: String,
    status: u16,
    remaining: usize,
}

/// A running fake API server.
pub struct FakeApi {
    /// Base URL, e.g. `http://127.0.0.1:1234`.
    pub url: String,
    state: Arc<Mutex<State>>,
    events: broadcast::Sender<(String, Value)>,
    shutdown: CancellationToken,
}

impl Drop for FakeApi {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Where an object path lives: `(prefix, namespace, plural)`.
fn collection_of(key: &str) -> Option<(String, String, String)> {
    let segments: Vec<&str> = key.trim_start_matches('/').split('/').collect();
    let prefix_len = if segments.first() == Some(&"api") { 2 } else { 3 };
    let rest = segments.get(prefix_len..)?;
    match rest {
        ["namespaces", namespace, plural, _name] => {
            Some((segments[..prefix_len].join("/"), (*namespace).to_owned(), (*plural).to_owned()))
        }
        _ => None,
    }
}

enum Route {
    Discovery,
    Collection { prefix: String, namespace: Option<String>, plural: String },
    Object { key: String, sub: Option<String> },
    Unknown,
}

fn route(path: &str) -> Route {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let prefix_len = match segments.first() {
        Some(&"api") => 2,
        Some(&"apis") => 3,
        _ => return Route::Unknown,
    };
    if segments.len() < prefix_len {
        return Route::Unknown;
    }
    let prefix = segments[..prefix_len].join("/");
    match &segments[prefix_len..] {
        [] => Route::Discovery,
        [plural] => Route::Collection { prefix, namespace: None, plural: (*plural).to_owned() },
        ["namespaces", namespace, plural] => {
            Route::Collection { prefix, namespace: Some((*namespace).to_owned()), plural: (*plural).to_owned() }
        }
        ["namespaces", namespace, plural, name] => {
            Route::Object { key: format!("/{prefix}/namespaces/{namespace}/{plural}/{name}"), sub: None }
        }
        ["namespaces", namespace, plural, name, sub] => Route::Object {
            key: format!("/{prefix}/namespaces/{namespace}/{plural}/{name}"),
            sub: Some((*sub).to_owned()),
        },
        _ => Route::Unknown,
    }
}

/// JSON merge patch (RFC 7386).
pub fn merge(target: &mut Value, patch: &Value) {
    if let Value::Object(patch) = patch {
        if !target.is_object() {
            *target = json!({});
        }
        let target = target.as_object_mut().unwrap();
        for (key, value) in patch {
            if value.is_null() {
                target.remove(key);
            } else {
                merge(target.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
    } else {
        *target = patch.clone();
    }
}

fn full(status: StatusCode, body: &Value) -> Response<Body> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())).boxed_unsync());
    *response.status_mut() = status;
    response.headers_mut().insert("content-type", "application/json".parse().unwrap());
    response
}

fn status(code: u16, reason: &str, message: &str) -> Response<Body> {
    full(
        StatusCode::from_u16(code).unwrap(),
        &json!({
            "kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": message, "reason": reason, "code": code
        }),
    )
}

/// Builds an `APIResourceList` entry list for discovery.
pub fn resource(plural: &str, kind: &str, scale: bool) -> Vec<Value> {
    let mut resources = vec![json!({
        "name": plural, "singularName": kind.to_lowercase(), "namespaced": true, "kind": kind,
        "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]
    })];
    if scale {
        resources.push(json!({
            "name": format!("{plural}/scale"), "singularName": "", "namespaced": true,
            "group": "autoscaling", "version": "v1", "kind": "Scale", "verbs": ["get", "patch", "update"]
        }));
    }
    resources
}

impl FakeApi {
    /// Starts the server on an ephemeral port with common API groups registered.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (events, _) = broadcast::channel(1024);
        let api = Self { url, state: Arc::default(), events, shutdown: CancellationToken::new() };
        api.register(
            "apps/v1",
            [resource("deployments", "Deployment", true), resource("statefulsets", "StatefulSet", true)].concat(),
        );
        api.register("autoscaling/v2", resource("horizontalpodautoscalers", "HorizontalPodAutoscaler", false));
        api.register("keda.sh/v1alpha1", resource("scaledobjects", "ScaledObject", false));
        api.register("v1", resource("configmaps", "ConfigMap", false));
        let (state, events, shutdown) = (Arc::clone(&api.state), api.events.clone(), api.shutdown.clone());
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    () = shutdown.cancelled() => return,
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                let (state, events, shutdown) = (Arc::clone(&state), events.clone(), shutdown.clone());
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        handle(Arc::clone(&state), events.clone(), shutdown.clone(), request)
                    });
                    let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                });
            }
        });
        api
    }

    /// Registers discovery for a group version (`v1` for the core group).
    pub fn register(&self, group_version: &str, resources: Vec<Value>) {
        let path = if group_version.contains('/') {
            format!("/apis/{group_version}")
        } else {
            format!("/api/{group_version}")
        };
        let list = json!({"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": group_version, "resources": resources});
        self.state.lock().unwrap().discovery.insert(path, list);
    }

    /// A client talking to this server with `default` as namespace.
    pub fn client(&self) -> kube::Client {
        kube::Client::try_from(kube::Config::new(self.url.parse().unwrap())).unwrap()
    }

    /// Writes a kubeconfig pointing at this server into `dir`.
    pub fn kubeconfig(&self, dir: &Path) -> PathBuf {
        let path = dir.join("kubeconfig");
        let config = format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: fake\n  cluster:\n    server: {}\nusers:\n- name: fake\n  user: {{}}\n\
             contexts:\n- name: fake\n  context:\n    cluster: fake\n    user: fake\n    namespace: default\ncurrent-context: fake\n",
            self.url
        );
        std::fs::write(&path, config).unwrap();
        path
    }

    /// Stores an object at its API path, filling in server-side metadata.
    pub fn insert(&self, key: &str, mut object: Value) {
        let mut state = self.state.lock().unwrap();
        state.version += 1;
        let version = state.version;
        let metadata = &mut object["metadata"];
        merge(
            metadata,
            &json!({"resourceVersion": version.to_string(), "uid": format!("uid-{version}"), "creationTimestamp": "2026-01-01T00:00:00Z"}),
        );
        if metadata.get("generation").is_none() {
            metadata["generation"] = json!(1);
        }
        let existed = state.objects.insert(key.to_owned(), object.clone()).is_some();
        let _ = self
            .events
            .send((key.to_owned(), json!({"type": if existed { "MODIFIED" } else { "ADDED" }, "object": object})));
    }

    /// Returns a stored object.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.state.lock().unwrap().objects.get(key).cloned()
    }

    /// Removes a stored object.
    pub fn remove(&self, key: &str) {
        if let Some(object) = self.state.lock().unwrap().objects.remove(key) {
            let _ = self.events.send((key.to_owned(), json!({"type": "DELETED", "object": object})));
        }
    }

    /// Fails the next `times` requests whose method matches and whose path contains `path`.
    pub fn fail(&self, method: &str, path: &str, status: u16, times: usize) {
        self.state.lock().unwrap().failures.push(Failure {
            method: method.parse().unwrap(),
            path: path.to_owned(),
            status,
            remaining: times,
        });
    }

    /// Requests received so far, as `METHOD /path`.
    pub fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }

    /// Number of received requests starting with `prefix`.
    pub fn count(&self, prefix: &str) -> usize {
        self.requests().iter().filter(|request| request.starts_with(prefix)).count()
    }
}

async fn handle(
    state: Arc<Mutex<State>>,
    events: broadcast::Sender<(String, Value)>,
    shutdown: CancellationToken,
    request: Request<Incoming>,
) -> Result<Response<Body>, Infallible> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().unwrap_or_default().to_owned();
    let body = request.into_body().collect().await.map(|body| body.to_bytes()).unwrap_or_default();
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);

    let mut guard = state.lock().unwrap();
    guard.requests.push(format!("{method} {path}"));
    if let Some(failure) =
        guard.failures.iter_mut().find(|f| f.remaining > 0 && f.method == method && path.contains(&f.path))
    {
        failure.remaining -= 1;
        return Ok(status(failure.status, "InternalError", "injected failure"));
    }

    let response = match (route(&path), &method) {
        (Route::Discovery, &Method::GET) => match guard.discovery.get(&path) {
            Some(list) => full(StatusCode::OK, list),
            None => status(404, "NotFound", "group version not served"),
        },
        (Route::Collection { prefix, namespace, plural }, &Method::GET) => {
            let matches = |key: &str| {
                collection_of(key).is_some_and(|(p, ns, pl)| {
                    p == prefix && pl == plural && namespace.as_ref().is_none_or(|n| *n == ns)
                })
            };
            if query.contains("watch=true") {
                drop(guard);
                return Ok(watch(events.subscribe(), shutdown, matches_owned(prefix, namespace, plural)));
            }
            let items: Vec<Value> =
                guard.objects.iter().filter(|(key, _)| matches(key)).map(|(_, o)| o.clone()).collect();
            full(
                StatusCode::OK,
                &json!({"kind": "List", "apiVersion": "v1", "metadata": {"resourceVersion": guard.version.to_string()}, "items": items}),
            )
        }
        (Route::Collection { prefix, namespace, plural }, &Method::POST) => {
            let namespace = namespace.unwrap_or_default();
            let mut object = body;
            let name = object["metadata"]["name"].as_str().map_or_else(
                || format!("{}{}", object["metadata"]["generateName"].as_str().unwrap_or("object-"), guard.version + 1),
                str::to_owned,
            );
            let key = format!("/{prefix}/namespaces/{namespace}/{plural}/{name}");
            if guard.objects.contains_key(&key) {
                status(409, "AlreadyExists", "already exists")
            } else {
                merge(&mut object["metadata"], &json!({"name": name, "namespace": namespace}));
                drop(guard);
                return Ok(store(&state, &events, &key, object, StatusCode::CREATED));
            }
        }
        (Route::Object { key, sub: None }, &Method::GET) => match guard.objects.get(&key) {
            Some(object) => full(StatusCode::OK, object),
            None => status(404, "NotFound", "not found"),
        },
        (Route::Object { key, sub: Some(sub) }, &Method::GET) if sub == "scale" => match guard.objects.get(&key) {
            Some(object) => full(StatusCode::OK, &scale_of(object)),
            None => status(404, "NotFound", "not found"),
        },
        (Route::Object { key, sub }, &Method::PATCH) => {
            let apply = sub.is_none();
            match guard.objects.get(&key).cloned() {
                Some(mut object) => {
                    match sub.as_deref() {
                        Some("scale") => {
                            let mut scale = scale_of(&object);
                            merge(&mut scale, &body);
                            object["spec"]["replicas"] = scale["spec"]["replicas"].clone();
                        }
                        Some(_) => merge(&mut object["status"], &body["status"]),
                        None => {
                            let spec = object["spec"].clone();
                            merge(&mut object, &body);
                            if object["spec"] != spec {
                                let generation = object["metadata"]["generation"].as_i64().unwrap_or(1);
                                object["metadata"]["generation"] = json!(generation + 1);
                            }
                        }
                    }
                    drop(guard);
                    let response = store(&state, &events, &key, object, StatusCode::OK);
                    if sub.as_deref() == Some("scale") {
                        let object = state.lock().unwrap().objects[&key].clone();
                        return Ok(full(StatusCode::OK, &scale_of(&object)));
                    }
                    return Ok(response);
                }
                None if apply => {
                    drop(guard);
                    let mut object = body;
                    let (_, namespace, _) = collection_of(&key).unwrap();
                    merge(
                        &mut object["metadata"],
                        &json!({"name": key.rsplit('/').next().unwrap(), "namespace": namespace}),
                    );
                    return Ok(store(&state, &events, &key, object, StatusCode::CREATED));
                }
                None => status(404, "NotFound", "not found"),
            }
        }
        (Route::Object { key, sub: None }, &Method::PUT) => {
            drop(guard);
            return Ok(store(&state, &events, &key, body, StatusCode::OK));
        }
        _ => status(404, "NotFound", "unsupported request"),
    };
    Ok(response)
}

fn matches_owned(prefix: String, namespace: Option<String>, plural: String) -> impl Fn(&str) -> bool + Send + 'static {
    move |key: &str| {
        collection_of(key)
            .is_some_and(|(p, ns, pl)| p == prefix && pl == plural && namespace.as_ref().is_none_or(|n| *n == ns))
    }
}

fn scale_of(object: &Value) -> Value {
    json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": {"name": object["metadata"]["name"], "namespace": object["metadata"]["namespace"], "resourceVersion": object["metadata"]["resourceVersion"]},
        "spec": {"replicas": object["spec"]["replicas"].as_i64().unwrap_or(0)},
        "status": {"replicas": object["status"]["replicas"].as_i64().unwrap_or(0)}
    })
}

fn store(
    state: &Arc<Mutex<State>>,
    events: &broadcast::Sender<(String, Value)>,
    key: &str,
    mut object: Value,
    code: StatusCode,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    state.version += 1;
    let version = state.version;
    let existed = state.objects.contains_key(key);
    merge(&mut object["metadata"], &json!({"resourceVersion": version.to_string()}));
    if !existed {
        merge(
            &mut object["metadata"],
            &json!({"uid": format!("uid-{version}"), "generation": 1, "creationTimestamp": "2026-01-01T00:00:00Z"}),
        );
    }
    state.objects.insert(key.to_owned(), object.clone());
    let _ =
        events.send((key.to_owned(), json!({"type": if existed { "MODIFIED" } else { "ADDED" }, "object": object})));
    full(code, &object)
}

fn watch(
    receiver: broadcast::Receiver<(String, Value)>,
    shutdown: CancellationToken,
    matches: impl Fn(&str) -> bool + Send + 'static,
) -> Response<Body> {
    let stream =
        futures::stream::unfold((receiver, shutdown, matches), |(mut receiver, shutdown, matches)| async move {
            loop {
                let message = tokio::select! {
                    () = shutdown.cancelled() => return None,
                    message = receiver.recv() => message,
                };
                match message {
                    Ok((key, event)) if matches(&key) => {
                        let line = Bytes::from(format!("{event}\n"));
                        return Some((Ok(Frame::data(line)), (receiver, shutdown, matches)));
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
    let mut response = Response::new(StreamBody::new(stream).boxed_unsync());
    response.headers_mut().insert("content-type", "application/json".parse().unwrap());
    response
}
