//! `POST /v1/chat/completions` against scripted in-process mock upstreams
//! (axum servers on ephemeral ports). No test touches a real provider.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use serde_json::{Value, json};
use sluice::AppState;
use sluice::config::Config;
use tokio::net::TcpListener;

const ALPHA_KEY: &str = "alpha-upstream-key-0001";
const BETA_KEY: &str = "beta-upstream-key-0002";
const PAID_KEY: &str = "paid-upstream-key-0003";
/// What a client sends as its own key; must never reach a provider.
const CLIENT_SECRET: &str = "client-own-secret-key-9999";

// ---------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Reply {
    /// One complete response.
    Full {
        status: u16,
        headers: Vec<(&'static str, &'static str)>,
        body: Vec<u8>,
    },
    /// Reply `n` to call `n` (the last one repeats).
    Sequence(Vec<Reply>),
    /// Wait before replying.
    Delayed(Duration, Box<Reply>),
    /// Stream chunks, then abort the connection mid-body.
    StreamThenAbort(Vec<(Duration, Vec<u8>)>),
    /// `text/event-stream` body: each chunk is sent after its delay.
    Stream(Vec<(Duration, Vec<u8>)>),
}

impl Reply {
    fn json(status: u16, body: &str) -> Reply {
        Reply::Full {
            status,
            headers: vec![("content-type", "application/json")],
            body: body.as_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
struct Captured {
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

impl Captured {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("upstream received invalid JSON")
    }
}

/// Sets its flag when dropped; lives inside the mock's response stream so
/// the test can see when the upstream stream is torn down.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct MockState {
    reply: Reply,
    captured: Arc<Mutex<Vec<Captured>>>,
    /// Instant each stream chunk was handed to the server, latest request.
    emitted: Arc<Mutex<Vec<Instant>>>,
    stream_dropped: Arc<AtomicBool>,
}

struct Mock {
    addr: SocketAddr,
    state: MockState,
}

impl Mock {
    async fn start(reply: Reply) -> Mock {
        let state = MockState {
            reply,
            captured: Arc::default(),
            emitted: Arc::default(),
            stream_dropped: Arc::default(),
        };
        let app = Router::new()
            .fallback(mock_handler)
            .layer(DefaultBodyLimit::disable())
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        Mock { addr, state }
    }

    fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    fn calls(&self) -> Vec<Captured> {
        self.state.captured.lock().unwrap().clone()
    }

    fn only_call(&self) -> Captured {
        let calls = self.calls();
        assert_eq!(calls.len(), 1, "expected exactly one upstream call");
        calls.into_iter().next().unwrap()
    }

    fn emitted(&self) -> Vec<Instant> {
        self.state.emitted.lock().unwrap().clone()
    }
}

async fn mock_handler(
    State(m): State<MockState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let n = {
        let mut calls = m.captured.lock().unwrap();
        calls.push(Captured {
            path: uri.path().to_string(),
            headers,
            body,
        });
        calls.len() - 1
    };
    let mut reply = m.reply.clone();
    loop {
        match reply {
            Reply::Sequence(list) => {
                reply = list[n.min(list.len() - 1)].clone();
            }
            Reply::Delayed(wait, inner) => {
                tokio::time::sleep(wait).await;
                reply = *inner;
            }
            other => {
                reply = other;
                break;
            }
        }
    }
    match reply {
        Reply::Sequence(_) | Reply::Delayed(..) => unreachable!(),
        Reply::StreamThenAbort(chunks) => {
            let stream = futures_util::stream::unfold(chunks.into_iter(), |mut it| async move {
                match it.next() {
                    Some((gap, chunk)) => {
                        tokio::time::sleep(gap).await;
                        Some((Ok::<_, std::io::Error>(Bytes::from(chunk)), it))
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Some((Err(std::io::Error::other("upstream dropped")), it))
                    }
                }
            });
            let mut r = Response::new(Body::from_stream(stream));
            r.headers_mut()
                .insert("content-type", "text/event-stream".parse().unwrap());
            r
        }
        Reply::Full {
            status,
            headers,
            body,
        } => {
            let mut r = Response::new(Body::from(body));
            *r.status_mut() = StatusCode::from_u16(status).unwrap();
            for (k, v) in headers {
                r.headers_mut().append(k, v.parse().unwrap());
            }
            r
        }
        Reply::Stream(chunks) => {
            m.emitted.lock().unwrap().clear();
            m.stream_dropped.store(false, Ordering::SeqCst);
            let seed = (
                chunks.into_iter(),
                m.emitted.clone(),
                DropFlag(m.stream_dropped.clone()),
            );
            let stream = futures_util::stream::unfold(seed, |(mut it, emitted, flag)| async move {
                let (gap, chunk) = it.next()?;
                tokio::time::sleep(gap).await;
                emitted.lock().unwrap().push(Instant::now());
                Some((Ok::<_, Infallible>(Bytes::from(chunk)), (it, emitted, flag)))
            });
            let mut r = Response::new(Body::from_stream(stream));
            r.headers_mut()
                .insert("content-type", "text/event-stream".parse().unwrap());
            r
        }
    }
}

/// An address on which nothing is listening.
async fn closed_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

// ---------------------------------------------------------------------------
// Router under test
// ---------------------------------------------------------------------------

/// Config with providers `alpha` (allowlist), `beta` (no allowlist) and the
/// paid `money`, pointing at the given base URLs, plus pool `coder`.
fn config(alpha: &str, beta: &str, extra: &str) -> Config {
    config_full("", alpha, beta, extra, ALPHA_KEY)
}

/// Like `config`, with top-level settings and alpha's `ALPHA_KEYS` value.
fn config_full(top: &str, alpha: &str, beta: &str, extra: &str, alpha_keys: &str) -> Config {
    let text = format!(
        r#"
{top}
[[provider]]
id = "alpha"
base_url = "{alpha}"
keys_env = "ALPHA_KEYS"
models = ["vendor/model-a", "vendor/some-model:free"]

[[provider]]
id = "beta"
base_url = "{beta}"
keys_env = "BETA_KEYS"
{extra}

[[provider]]
id = "money"
base_url = "{alpha}"
keys_env = "PAID_KEYS"
paid = true
models = ["vendor/model-a-pro", "vendor/model-a-noprice"]
[provider.prices."vendor/model-a-pro"]
in_per_mtok = 10.0
out_per_mtok = 20.0

[[pool]]
name = "coder"
routes = ["alpha/vendor/model-a", "beta/fallback-model"]
"#
    );
    let env: HashMap<&str, &str> = [
        ("ALPHA_KEYS", alpha_keys),
        ("BETA_KEYS", BETA_KEY),
        ("PAID_KEYS", PAID_KEY),
    ]
    .into();
    Config::from_toml_str(&text, &|k| env.get(k).map(|v| v.to_string()))
        .unwrap_or_else(|e| panic!("test config invalid: {e}"))
}

/// Like `start_router`, also returning the shared state. When `load` is set
/// the state file is loaded first, like a restart does.
async fn start_router_state(config: Config, load: bool) -> (SocketAddr, Arc<AppState>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(config).unwrap();
    if load {
        sluice::state::load(&state);
    }
    tokio::spawn(sluice::serve(
        listener,
        state.clone(),
        std::future::pending(),
    ));
    (addr, state)
}

async fn start_router(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(config).unwrap();
    tokio::spawn(sluice::serve(listener, state, std::future::pending()));
    addr
}

fn client() -> reqwest::Client {
    sluice::proxy::build_client().unwrap()
}

async fn post(router: SocketAddr, body: impl Into<Vec<u8>>) -> reqwest::Response {
    client()
        .post(format!("http://{router}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {CLIENT_SECRET}"))
        .body(body.into())
        .send()
        .await
        .unwrap()
}

async fn post_json(router: SocketAddr, body: &Value) -> reqwest::Response {
    post(router, serde_json::to_vec(body).unwrap()).await
}

async fn post_with(
    router: SocketAddr,
    body: &Value,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    let mut req = client()
        .post(format!("http://{router}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {CLIENT_SECRET}"))
        .body(serde_json::to_vec(body).unwrap());
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    req.send().await.unwrap()
}

/// A fresh state file path (TOML-literal-safe) that does not exist yet.
fn temp_state_file(name: &str) -> String {
    let path = std::env::temp_dir().join(format!("sluice-it-{}-{name}.json", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path.display().to_string().replace('\\', "/")
}

fn header<'a>(r: &'a reqwest::Response, name: &str) -> &'a str {
    r.headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .unwrap()
}

async fn error_body(r: reqwest::Response) -> Value {
    let bytes = r.bytes().await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v["error"]["message"].is_string(), "not OpenAI-shaped: {v}");
    assert!(v["error"]["type"].is_string(), "not OpenAI-shaped: {v}");
    v
}

/// Assert that no secret appears anywhere in what the upstream received
/// besides the provider key in `authorization`.
fn assert_no_client_secret(c: &Captured) {
    for (name, value) in &c.headers {
        let v = String::from_utf8_lossy(value.as_bytes());
        assert!(
            !v.contains(CLIENT_SECRET),
            "client secret leaked upstream in {name}"
        );
    }
    assert!(!String::from_utf8_lossy(&c.body).contains(CLIENT_SECRET));
}

// ---------------------------------------------------------------------------
// T1, T2, T3
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t1_non_streaming_body_identical_and_router_headers_present() {
    // Odd spacing, key order and escapes: any re-serialisation would show.
    let upstream_body = "{\"id\":\"cmpl-1\",  \"object\":\"chat.completion\",\n \"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"h\\u00e9llo\"}}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1},\"z\":1.50}";
    let alpha = Mock::start(Reply::Full {
        status: 200,
        headers: vec![
            ("content-type", "application/json"),
            ("x-ratelimit-remaining-requests", "41"),
            ("x-router-provider", "spoofed-by-upstream"),
        ],
        body: upstream_body.as_bytes().to_vec(),
    })
    .await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;

    let r = client()
        .post(format!("http://{router}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("authorization", format!("Bearer {CLIENT_SECRET}"))
        .header("cookie", "session=abc")
        .header("x-router-private", "1")
        .header("x-custom-client-header", "nope")
        .body(r#"{"model":"alpha/vendor/model-a","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "alpha");
    assert_eq!(header(&r, "x-router-upstream-model"), "vendor/model-a");
    assert_eq!(header(&r, "x-router-attempts"), "1");
    let overhead: f64 = header(&r, "x-router-overhead-ms").parse().unwrap();
    assert!((0.0..1000.0).contains(&overhead), "overhead {overhead}");
    assert_eq!(header(&r, "content-type"), "application/json");
    assert_eq!(header(&r, "x-ratelimit-remaining-requests"), "41");
    assert_eq!(r.headers().get_all("x-router-provider").iter().count(), 1);
    let body = r.bytes().await.unwrap();
    assert_eq!(
        &body[..],
        upstream_body.as_bytes(),
        "body not byte-identical"
    );

    let call = alpha.only_call();
    assert_eq!(call.path, "/v1/chat/completions");
    assert_eq!(
        call.headers["authorization"],
        format!("Bearer {ALPHA_KEY}").as_str()
    );
    assert_eq!(call.headers["content-type"], "application/json");
    assert_eq!(call.headers["accept"], "application/json");
    for absent in ["cookie", "x-router-private", "x-custom-client-header"] {
        assert!(
            call.headers.get(absent).is_none(),
            "{absent} forwarded upstream"
        );
    }
    assert_no_client_secret(&call);
    assert_eq!(call.json()["model"], "vendor/model-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t2_streaming_chunks_arrive_incrementally_and_fast() {
    const RUNS: usize = 5;
    const BOUND: Duration = Duration::from_millis(5);
    let chunk1 = b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n".to_vec();
    let chunk2 = b"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n".to_vec();
    let chunk3 = b"data: [DONE]\n\n".to_vec();
    let alpha = Mock::start(Reply::Stream(vec![
        // Let the response headers reach the client before chunk 1.
        (Duration::from_millis(100), chunk1.clone()),
        // Deliberate gap: chunk 1 must arrive before chunk 2 is even sent.
        (Duration::from_millis(300), chunk2.clone()),
        (Duration::from_millis(20), chunk3.clone()),
    ]))
    .await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;
    let expected: Vec<u8> = [chunk1.clone(), chunk2, chunk3].concat();
    let request = json!({"model": "alpha/vendor/model-a", "stream": true,
                         "messages": [{"role": "user", "content": "hi"}]});

    let mut latencies = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let mut r = post_json(router, &request).await;
        assert_eq!(r.status(), 200);
        assert_eq!(header(&r, "content-type"), "text/event-stream");
        assert_eq!(header(&r, "x-router-provider"), "alpha");
        assert_eq!(header(&r, "x-router-attempts"), "1");

        let mut received = Vec::new();
        let mut first_complete_at = None;
        while let Some(chunk) = r.chunk().await.unwrap() {
            received.extend_from_slice(&chunk);
            if first_complete_at.is_none() && received.len() >= chunk1.len() {
                first_complete_at = Some((Instant::now(), received.len()));
            }
        }
        assert_eq!(received, expected, "stream not forwarded unchanged");

        let (first_at, len_then) = first_complete_at.unwrap();
        assert_eq!(
            len_then,
            chunk1.len(),
            "chunk 1 was not delivered on its own"
        );
        let emitted = alpha.emitted();
        assert_eq!(emitted.len(), 3);
        assert!(
            first_at < emitted[1],
            "client saw chunk 1 only after the mock sent chunk 2"
        );
        latencies.push(first_at.saturating_duration_since(emitted[0]));
    }

    let best = latencies.iter().min().copied().unwrap();
    assert!(
        best < BOUND,
        "first chunk forwarded in {best:?} (best of {RUNS}: {latencies:?}), bound {BOUND:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t3_tool_call_request_reaches_upstream_unchanged_except_model() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;

    let request = json!({
        "model": "coder",
        "messages": [
            {"role": "system", "content": "You are terse."},
            {"role": "user", "content": "What is the weather in Paris?"},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_1", "type": "function",
                "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": "{\"temp_c\": 21.5}"},
            {"role": "user", "content": [{"type": "text", "text": "And tomorrow?"}]}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Weather for a city",
                "parameters": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}, "days": {"type": "integer", "minimum": 1}},
                    "required": ["city"],
                    "additionalProperties": false
                },
                "strict": true
            }
        }],
        "tool_choice": {"type": "function", "function": {"name": "get_weather"}},
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "answer", "schema": {"type": "object"}, "strict": true}
        },
        "temperature": 0.25,
        "max_tokens": 512,
        "parallel_tool_calls": false,
        "x_unknown_vendor_field": {"nested": [1, 2.5, "three", null, true]}
    });
    let r = post_json(router, &request).await;
    assert_eq!(r.status(), 200);

    let mut expected = request.clone();
    expected["model"] = json!("vendor/model-a");
    assert_eq!(alpha.only_call().json(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t3_drop_params_removes_only_listed_top_level_fields() {
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(
        "http://127.0.0.1:9/v1",
        &beta.base_url(),
        r#"drop_params = ["logprobs", "seed", "not_present"]"#,
    ))
    .await;

    let request = json!({
        "model": "beta/m",
        "messages": [{"role": "user", "content": "hi", "seed": "nested, kept"}],
        "logprobs": true,
        "seed": 7,
        "top_p": 0.9
    });
    let r = post_json(router, &request).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        beta.only_call().json(),
        json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi", "seed": "nested, kept"}],
            "top_p": 0.9
        })
    );
}

// ---------------------------------------------------------------------------
// T18, T19
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t18_explicit_route_calls_only_the_named_provider() {
    let alpha = Mock::start(Reply::json(200, "{\"from\":\"alpha\"}")).await;
    let beta = Mock::start(Reply::json(200, "{\"from\":\"beta\"}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    let r = post_json(router, &json!({"model": "beta/some-model", "messages": []})).await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "beta");
    assert_eq!(r.text().await.unwrap(), "{\"from\":\"beta\"}");
    assert_eq!(beta.calls().len(), 1);
    assert_eq!(alpha.calls().len(), 0);
    let call = beta.only_call();
    assert_eq!(
        call.headers["authorization"],
        format!("Bearer {BETA_KEY}").as_str()
    );
    assert_eq!(call.json()["model"], "some-model");

    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-a", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "alpha");
    assert_eq!(alpha.calls().len(), 1);
    assert_eq!(beta.calls().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t19_unknown_model_is_404_with_suggestions_and_no_upstream_call() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    for (model, suggestion) in [
        ("codr", "coder"),
        ("vendor/model-a", "alpha/vendor/model-a"),
        ("gamma/vendor/model-a", "alpha/vendor/model-a"),
    ] {
        let r = post_json(router, &json!({"model": model, "messages": []})).await;
        assert_eq!(r.status(), 404, "{model}");
        let v = error_body(r).await;
        assert_eq!(v["error"]["code"], "model_not_found");
        assert_eq!(v["error"]["param"], "model");
        let list = v["error"]["suggestions"].as_array().unwrap();
        assert!(list.contains(&json!(suggestion)), "{model}: {v}");
        assert!(v["error"]["message"].as_str().unwrap().contains(suggestion));
    }

    let r = post_json(
        router,
        &json!({"model": "zzzzzzzzzzzzzzzzzzzz", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 404);
    assert_eq!(error_body(r).await["error"]["suggestions"], json!([]));

    assert!(alpha.calls().is_empty());
    assert!(beta.calls().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t19_model_outside_allowlist_is_404_with_suggestions_and_no_upstream_call() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;

    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-b", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 404);
    let v = error_body(r).await;
    assert_eq!(v["error"]["code"], "model_not_found");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("allowlist")
    );
    assert_eq!(v["error"]["suggestions"][0], "alpha/vendor/model-a");
    assert!(alpha.calls().is_empty());
}

// ---------------------------------------------------------------------------
// Other M1 behaviour
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_error_statuses_pass_through_unchanged() {
    for (status, body) in [
        (
            400,
            "{\"error\":{\"message\":\"bad\",\"type\":\"invalid_request_error\"}}",
        ),
        (401, "{\"error\":\"no\"}"),
        (404, "not json at all"),
        (429, "{\"error\":{\"message\":\"slow down\"}}"),
        (500, "{\"error\":\"boom\"}"),
        (503, ""),
    ] {
        let alpha = Mock::start(Reply::Full {
            status,
            headers: vec![("content-type", "application/json"), ("retry-after", "30")],
            body: body.as_bytes().to_vec(),
        })
        .await;
        let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;
        // Retryable statuses with one key and no other route are returned once
        // (the last upstream error); the route is then cooling or disabled.
        let retryable = [401, 429, 500, 503].contains(&status);
        let streams: &[bool] = if retryable { &[false] } else { &[false, true] };
        for &stream in streams {
            let r = post_json(
                router,
                &json!({"model": "alpha/vendor/model-a", "stream": stream, "messages": []}),
            )
            .await;
            assert_eq!(r.status().as_u16(), status);
            assert_eq!(header(&r, "retry-after"), "30");
            assert_eq!(header(&r, "x-router-provider"), "alpha");
            assert_eq!(header(&r, "x-router-attempts"), "1");
            assert_eq!(r.bytes().await.unwrap(), body.as_bytes(), "{status}");
        }
        assert_eq!(alpha.calls().len(), streams.len());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_upstream_is_502_without_details() {
    let dead = closed_addr().await;
    let router = start_router(config(
        &format!("http://{dead}/v1"),
        "http://127.0.0.1:9/v1",
        "",
    ))
    .await;
    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-a", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 502);
    let v = error_body(r).await;
    assert_eq!(v["error"]["code"], "upstream_unreachable");
    let text = v.to_string();
    assert!(!text.contains(ALPHA_KEY));
    assert!(!text.contains(&dead.port().to_string()), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_model_with_extra_slashes_is_passed_verbatim() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    // Allowlisted on alpha, and free-form on beta (no allowlist).
    for (model, mock, upstream) in [
        (
            "alpha/vendor/some-model:free",
            &alpha,
            "vendor/some-model:free",
        ),
        ("beta/org/vendor/x:free/y", &beta, "org/vendor/x:free/y"),
    ] {
        let r = post_json(router, &json!({"model": model, "messages": []})).await;
        assert_eq!(r.status(), 200, "{model}");
        assert_eq!(header(&r, "x-router-upstream-model"), upstream);
        assert_eq!(mock.only_call().json()["model"], upstream);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_fails_over_to_next_route_on_500() {
    let alpha = Mock::start(Reply::json(500, "{\"error\":\"down\"}")).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    let r = post_json(router, &json!({"model": "coder", "messages": []})).await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "beta");
    assert_eq!(header(&r, "x-router-upstream-model"), "fallback-model");
    assert_eq!(header(&r, "x-router-attempts"), "2");
    assert_eq!(alpha.only_call().json()["model"], "vendor/model-a");
    assert_eq!(beta.only_call().json()["model"], "fallback-model");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_requests_are_400_without_upstream_call() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;

    for (body, code) in [
        ("{not json", "invalid_json"),
        ("", "invalid_json"),
        ("[1, 2]", "invalid_request"),
        ("{\"messages\": []}", "missing_model"),
        ("{\"model\": 42}", "invalid_model"),
        ("{\"model\": null}", "invalid_model"),
    ] {
        let r = post(router, body).await;
        assert_eq!(r.status(), 400, "{body}");
        assert_eq!(error_body(r).await["error"]["code"], code, "{body}");
    }
    assert!(alpha.calls().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn base_url_trailing_slash_is_handled() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(
        &format!("{}/", alpha.base_url()),
        "http://127.0.0.1:9/v1",
        "",
    ))
    .await;
    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-a", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(alpha.only_call().path, "/v1/chat/completions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_bodies_beyond_axum_default_limit_are_accepted() {
    let alpha = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;
    let big = "x".repeat(8 * 1024 * 1024);
    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-a", "messages": [{"role": "user", "content": big}]}),
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        alpha.only_call().json()["messages"][0]["content"]
            .as_str()
            .map(str::len),
        Some(big.len())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t17_client_disconnect_mid_stream_cancels_the_upstream_request() {
    let chunks = (0..200)
        .map(|i| {
            (
                Duration::from_millis(20),
                format!("data: {i}\n\n").into_bytes(),
            )
        })
        .collect();
    let alpha = Mock::start(Reply::Stream(chunks)).await;
    let router = start_router(config(&alpha.base_url(), "http://127.0.0.1:9/v1", "")).await;

    let mut r = post_json(
        router,
        &json!({"model": "alpha/vendor/model-a", "stream": true, "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 200);
    assert!(r.chunk().await.unwrap().is_some());
    assert!(!alpha.state.stream_dropped.load(Ordering::SeqCst));
    drop(r);

    // The full stream would take 4 s; the upstream must be torn down well
    // before that.
    let deadline = Instant::now() + Duration::from_secs(2);
    while !alpha.state.stream_dropped.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "upstream stream still alive after client disconnect"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(alpha.emitted().len() < 100);
}

// ---------------------------------------------------------------------------
// M2: pools, retries, cooldowns, breaker, missing routes (T4-T9, T12, T21)
// ---------------------------------------------------------------------------

const SSE_CHUNK: &[u8] = b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n";

fn auth_of(c: &Captured) -> String {
    c.headers["authorization"].to_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t4_429_with_retry_after_fails_over_and_cools_the_route() {
    let alpha = Mock::start(Reply::Full {
        status: 429,
        headers: vec![("content-type", "application/json"), ("retry-after", "30")],
        body: b"{\"error\":\"slow\"}".to_vec(),
    })
    .await;
    let beta = Mock::start(Reply::json(200, "{\"ok\":true}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;
    let ask = || async move { post_json(router, &json!({"model": "coder", "messages": []})).await };

    let r = ask().await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "beta");
    assert_eq!(header(&r, "x-router-attempts"), "2");

    // Cooling: alpha is not contacted again. (The exact 30 s expiry is
    // asserted in the limiter unit tests.)
    let r = ask().await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-attempts"), "1");
    assert_eq!(alpha.calls().len(), 1);
    assert_eq!(beta.calls().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t6_first_byte_timeout_fails_over() {
    let alpha = Mock::start(Reply::Delayed(
        Duration::from_secs(4),
        Box::new(Reply::json(200, "{\"slow\":true}")),
    ))
    .await;
    let beta = Mock::start(Reply::json(200, "{\"fast\":true}")).await;
    let router = start_router(config_full(
        "first_byte_timeout_secs = 1",
        &alpha.base_url(),
        &beta.base_url(),
        "",
        ALPHA_KEY,
    ))
    .await;

    let started = Instant::now();
    let r = post_json(router, &json!({"model": "coder", "messages": []})).await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "beta");
    assert_eq!(header(&r, "x-router-attempts"), "2");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(r.bytes().await.unwrap(), "{\"fast\":true}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t7_mid_stream_drop_emits_sse_error_event_and_does_not_retry() {
    let alpha = Mock::start(Reply::StreamThenAbort(vec![(
        Duration::from_millis(10),
        SSE_CHUNK.to_vec(),
    )]))
    .await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    let r = post_json(
        router,
        &json!({"model": "coder", "stream": true, "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-provider"), "alpha");
    let text = r.text().await.unwrap();
    assert!(text.starts_with(std::str::from_utf8(SSE_CHUNK).unwrap()));
    let (_, tail) = text.split_once("event: error\ndata: ").expect(&text);
    let data: Value = serde_json::from_str(tail.trim_end()).unwrap();
    assert_eq!(data["error"]["type"], "upstream_error");
    assert!(data["error"]["message"].is_string());
    assert_eq!(alpha.calls().len(), 1);
    assert!(
        beta.calls().is_empty(),
        "must not retry after streaming began"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t8_401_disables_the_key_and_the_next_key_is_used() {
    let alpha = Mock::start(Reply::Sequence(vec![
        Reply::json(401, "{\"error\":\"bad key\"}"),
        Reply::json(200, "{\"ok\":true}"),
    ]))
    .await;
    let keys = format!("{ALPHA_KEY},{BETA_KEY}");
    let router = start_router(config_full(
        "",
        &alpha.base_url(),
        "http://127.0.0.1:9/v1",
        "",
        &keys,
    ))
    .await;
    let ask = || async move {
        let body = json!({"model": "alpha/vendor/model-a", "messages": []});
        post_json(router, &body).await
    };

    let r = ask().await;
    assert_eq!(r.status(), 200);
    assert_eq!(header(&r, "x-router-attempts"), "2");
    let first = alpha.calls();
    let rejected = auth_of(&first[0]);
    let accepted = auth_of(&first[1]);
    assert_ne!(rejected, accepted);

    // The rejected key is never used again, whatever the rotation says.
    for _ in 0..3 {
        assert_eq!(ask().await.status(), 200);
    }
    let calls = alpha.calls();
    assert_eq!(calls.len(), 5);
    assert!(calls[2..].iter().all(|c| auth_of(c) == accepted));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t9_400_is_returned_unchanged_without_retry_or_cooldown() {
    let body = "{\"error\":{\"message\":\"bad param\"}}";
    let alpha = Mock::start(Reply::json(400, body)).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    for _ in 0..2 {
        let r = post_json(router, &json!({"model": "coder", "messages": []})).await;
        assert_eq!(r.status(), 400);
        assert_eq!(header(&r, "x-router-attempts"), "1");
        assert_eq!(r.bytes().await.unwrap(), body.as_bytes());
    }
    // No cooldown: the second request went to alpha again; beta never used.
    assert_eq!(alpha.calls().len(), 2);
    assert!(beta.calls().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t12_all_routes_cooling_gives_503_with_reasons_and_retry_after() {
    let rate_limited = |secs: &'static str| Reply::Full {
        status: 429,
        headers: vec![("content-type", "application/json"), ("retry-after", secs)],
        body: b"{\"error\":\"slow\"}".to_vec(),
    };
    let alpha = Mock::start(rate_limited("30")).await;
    let beta = Mock::start(rate_limited("60")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;
    let ask = || async move { post_json(router, &json!({"model": "coder", "messages": []})).await };

    // Every attempt fails with a retryable error: last upstream error returned.
    let r = ask().await;
    assert_eq!(r.status(), 429);
    assert_eq!(header(&r, "x-router-provider"), "beta");
    assert_eq!(header(&r, "x-router-attempts"), "2");

    let r = ask().await;
    assert_eq!(r.status(), 503);
    let retry_after: u64 = header(&r, "retry-after").parse().unwrap();
    assert!((29..=30).contains(&retry_after), "{retry_after}");
    let v = error_body(r).await;
    let routes = v["error"]["routes"].as_array().unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0]["route"], "alpha/vendor/model-a");
    assert_eq!(routes[0]["reason"], "cooldown");
    assert_eq!(routes[1]["route"], "beta/fallback-model");
    assert_eq!(routes[1]["reason"], "cooldown");
    assert!(routes[1]["retry_after_secs"].as_u64().unwrap() > 30);
    assert_eq!(alpha.calls().len(), 1);
    assert_eq!(beta.calls().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t21_404_on_a_pool_route_marks_it_missing_and_the_next_route_serves() {
    let alpha = Mock::start(Reply::json(404, "{\"error\":\"model gone\"}")).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), "")).await;

    for expected_attempts in ["2", "1"] {
        let r = post_json(router, &json!({"model": "coder", "messages": []})).await;
        assert_eq!(r.status(), 200);
        assert_eq!(header(&r, "x-router-provider"), "beta");
        assert_eq!(header(&r, "x-router-attempts"), expected_attempts);
    }
    assert_eq!(alpha.calls().len(), 1, "missing route is not retried");

    // On an explicit route a 404 goes back to the client, nothing is marked.
    let r = post_json(
        router,
        &json!({"model": "alpha/vendor/some-model:free", "messages": []}),
    )
    .await;
    assert_eq!(r.status(), 404);
    assert_eq!(alpha.calls().len(), 2);
}

// ---------------------------------------------------------------------------
// M3: scoped counters, pre-flight filter, usage capture, waiting (T10, T11, T20)
// ---------------------------------------------------------------------------

const DEAD: &str = "http://127.0.0.1:9/v1";

fn usage_reply() -> Reply {
    Reply::json(
        200,
        r#"{"id":"x","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t10_known_rpm_limit_skips_the_route_without_an_upstream_call() {
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let router = start_router(config(DEAD, &beta.base_url(), "[provider.limits]\nrpm = 1")).await;
    let ask = || async move {
        post_json(
            router,
            &json!({"model": "beta/fallback-model", "messages": []}),
        )
        .await
    };

    assert_eq!(ask().await.status(), 200);
    let r = ask().await;
    assert_eq!(r.status(), 503);
    let retry_after: u64 = header(&r, "retry-after").parse().unwrap();
    assert!((1..=60).contains(&retry_after), "{retry_after}");
    let v = error_body(r).await;
    assert_eq!(v["error"]["routes"][0]["reason"], "quota_rpm");
    assert_eq!(
        beta.calls().len(),
        1,
        "second request never left the router"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t11_waits_for_a_cooling_route_that_frees_within_max_wait() {
    let rate_limited = Reply::Full {
        status: 429,
        headers: vec![("content-type", "application/json"), ("retry-after", "1")],
        body: b"{}".to_vec(),
    };
    let script = Reply::Sequence(vec![rate_limited, Reply::json(200, "{}")]);
    let alpha = Mock::start(script.clone()).await;
    let beta = Mock::start(script).await;
    let cfg = config_full(
        "max_wait_secs = 5",
        &alpha.base_url(),
        &beta.base_url(),
        "",
        ALPHA_KEY,
    );
    let router = start_router(cfg).await;
    let ask = || async move { post_json(router, &json!({"model": "coder", "messages": []})).await };

    assert_eq!(ask().await.status(), 429, "both routes fail and cool down");
    let t = Instant::now();
    let r = ask().await;
    assert_eq!(r.status(), 200);
    let waited = t.elapsed();
    assert!(waited >= Duration::from_millis(900), "{waited:?}");
    assert!(waited < Duration::from_secs(4), "{waited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t20_provider_scope_limit_is_shared_by_two_models() {
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let extra = "[provider.limits]\nscope = \"provider\"\nrpm = 2";
    let router = start_router(config(DEAD, &beta.base_url(), extra)).await;
    let ask = |m: &'static str| async move {
        post_json(router, &json!({"model": m, "messages": []})).await
    };

    assert_eq!(ask("beta/model-one").await.status(), 200);
    assert_eq!(ask("beta/model-two").await.status(), 200);
    let r = ask("beta/model-one").await;
    assert_eq!(r.status(), 503);
    assert_eq!(
        error_body(r).await["error"]["routes"][0]["reason"],
        "quota_rpm"
    );
    assert_eq!(beta.calls().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_usage_is_requested_captured_and_hidden_unless_asked_for() {
    let sse = |s: &str| (Duration::ZERO, s.as_bytes().to_vec());
    let chunks = vec![
        sse("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n"),
        sse("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n"),
        sse("data: [DONE]\n\n"),
    ];
    for client_asks in [false, true] {
        let beta = Mock::start(Reply::Stream(chunks.clone())).await;
        let extra = "[provider.limits]\ntpm = 20";
        let router = start_router(config(DEAD, &beta.base_url(), extra)).await;
        let mut body = json!({"model": "beta/fallback-model", "stream": true, "messages": []});
        if client_asks {
            body["stream_options"] = json!({"include_usage": true});
        }
        let r = post_json(router, &body).await;
        assert_eq!(r.status(), 200);
        let text = r.text().await.unwrap();
        assert!(text.contains("\"content\":\"hi\""), "{text}");
        assert_eq!(text.contains("prompt_tokens"), client_asks, "{text}");
        assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
        let sent = beta.only_call().json();
        assert_eq!(sent["stream_options"]["include_usage"], true);

        // 15 tokens were used: 15 + the 1024 default allowance exceeds tpm = 20.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = post_json(router, &body).await;
        assert_eq!(r.status(), 503);
        assert_eq!(
            error_body(r).await["error"]["routes"][0]["reason"],
            "quota_tpm"
        );
    }
}

// ---------------------------------------------------------------------------
// M4: paid cap, privacy, persistence (T14, T15, T16)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t14_paid_cap_rejects_and_survives_a_restart() {
    let alpha = Mock::start(Reply::json(
        200,
        r#"{"usage":{"prompt_tokens":1000,"completion_tokens":1000}}"#,
    ))
    .await;
    let paid = |m: &'static str| json!({"model": m, "messages": []});

    // Cap 0 (the default) blocks paid routes entirely.
    let router = start_router(config(&alpha.base_url(), DEAD, "")).await;
    let r = post_json(router, &paid("money/vendor/model-a-pro")).await;
    assert_eq!(r.status(), 403);
    assert_eq!(error_body(r).await["error"]["code"], "paid_disabled");
    assert!(alpha.calls().is_empty());

    // Cap 0.001 USD; one request costs 1000 * 10/1M + 1000 * 20/1M = 0.03.
    let file = temp_state_file("t14");
    let top = format!("paid_daily_cap_usd = 0.001\nstate_file = '{file}'");
    let cfg = || config_full(&top, &alpha.base_url(), DEAD, "", ALPHA_KEY);
    let (router, state) = start_router_state(cfg(), false).await;
    let r = post_json(router, &paid("money/vendor/model-a-pro")).await;
    assert_eq!(r.status(), 200);
    let r = post_json(router, &paid("money/vendor/model-a-pro")).await;
    assert_eq!(r.status(), 403);
    assert_eq!(error_body(r).await["error"]["code"], "paid_cap_reached");
    let r = post_json(router, &paid("money/vendor/model-a-noprice")).await;
    assert_eq!(r.status(), 403);
    assert_eq!(error_body(r).await["error"]["code"], "paid_price_missing");
    assert_eq!(
        alpha.calls().len(),
        1,
        "only the first paid call went upstream"
    );

    sluice::state::save(&state).unwrap();
    let (router, _) = start_router_state(cfg(), true).await;
    let r = post_json(router, &paid("money/vendor/model-a-pro")).await;
    assert_eq!(r.status(), 403, "cap survives the restart");
    assert_eq!(error_body(r).await["error"]["code"], "paid_cap_reached");
    assert_eq!(alpha.calls().len(), 1);
    let _ = std::fs::remove_file(&file);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t15_private_requests_never_reach_training_providers() {
    let alpha = Mock::start(Reply::json(500, "{}")).await;
    let beta = Mock::start(Reply::json(200, "{}")).await;
    let extra = "trains_on_input = true";
    let router = start_router(config(&alpha.base_url(), &beta.base_url(), extra)).await;
    let private = [("x-router-private", "1")];

    let r = post_with(
        router,
        &json!({"model": "beta/m", "messages": []}),
        &private,
    )
    .await;
    assert_eq!(r.status(), 503);
    let v = error_body(r).await;
    assert_eq!(v["error"]["routes"][0]["reason"], "private_excluded");

    // In a pool the training provider is left out even when the first fails.
    let pool = json!({"model": "coder", "messages": []});
    assert_eq!(post_with(router, &pool, &private).await.status(), 500);
    assert!(beta.calls().is_empty());
    assert_eq!(post_with(router, &pool, &[]).await.status(), 200);
    assert_eq!(beta.calls().len(), 1);

    // private_default = true, with a per-request opt-out.
    let cfg = config_full(
        "private_default = true",
        DEAD,
        &beta.base_url(),
        extra,
        ALPHA_KEY,
    );
    let router = start_router(cfg).await;
    let one = json!({"model": "beta/m", "messages": []});
    assert_eq!(post_with(router, &one, &[]).await.status(), 503);
    assert_eq!(
        post_with(router, &one, &[("x-router-private", "0")])
            .await
            .status(),
        200
    );
    assert_eq!(beta.calls().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t16_restart_restores_day_counters_hourly_buckets_and_long_cooldowns() {
    let rate_limited = Reply::Full {
        status: 429,
        headers: vec![("content-type", "application/json"), ("retry-after", "120")],
        body: b"{}".to_vec(),
    };
    let alpha = Mock::start(rate_limited).await;
    let beta = Mock::start(usage_reply()).await;
    let file = temp_state_file("t16");
    let top = format!("state_file = '{file}'");
    let cfg = || {
        let extra = "[provider.limits]\nrpd = 2";
        config_full(&top, &alpha.base_url(), &beta.base_url(), extra, ALPHA_KEY)
    };
    let coder = json!({"model": "coder", "messages": []});

    let (router, state) = start_router_state(cfg(), false).await;
    for _ in 0..2 {
        let r = post_json(router, &coder).await;
        assert_eq!(r.status(), 200);
        assert_eq!(header(&r, "x-router-provider"), "beta");
    }
    assert_eq!(alpha.calls().len(), 1, "alpha cooled after its first 429");
    sluice::state::save(&state).unwrap();

    let (router, state) = start_router_state(cfg(), true).await;
    let r = post_json(router, &coder).await;
    assert_eq!(r.status(), 503);
    let v = error_body(r).await;
    assert_eq!(v["error"]["routes"][0]["reason"], "cooldown", "{v}");
    assert_eq!(v["error"]["routes"][1]["reason"], "quota_rpd", "{v}");
    assert_eq!(alpha.calls().len(), 1);
    assert_eq!(beta.calls().len(), 2);
    let hourly = state.limiter.hourly("beta", Instant::now());
    let totals = hourly.iter().fold((0, 0), |a, h| (a.0 + h.1, a.1 + h.2));
    assert_eq!(totals, (20, 10));
    let _ = std::fs::remove_file(&file);
}
