//! T27: router overhead against a local mock upstream (p95 under 2 ms).

use std::collections::HashMap;

use axum::Router;
use axum::routing::post;
use sluice::AppState;
use sluice::config::Config;
use tokio::net::TcpListener;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t27_router_overhead_p95_is_under_2ms() {
    let mock = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                [("content-type", "application/json")],
                r#"{"id":"x","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}"#,
            )
        }),
    );
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(upstream, mock).await });

    let env: HashMap<&str, &str> = [("MOCK_KEYS", "mock-key-00000001")].into();
    let text = format!(
        "[[provider]]\nid = \"mock\"\nbase_url = \"http://{upstream_addr}/v1\"\nkeys_env = \"MOCK_KEYS\"\nbilling = \"free\"\n"
    );
    let config = Config::from_toml_str(&text, &|k| env.get(k).map(|v| v.to_string())).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(config).unwrap();
    tokio::spawn(sluice::serve(listener, state, std::future::pending()));

    let client = sluice::proxy::build_client().unwrap();
    let body = r#"{"model":"mock/m","messages":[{"role":"user","content":"hi"}]}"#;
    let mut overheads = Vec::new();
    for i in 0..220 {
        let r = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let ms: f64 = r.headers()["x-router-overhead-ms"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let _ = r.bytes().await;
        if i >= 20 {
            overheads.push(ms); // the first 20 warm up connections
        }
    }
    overheads.sort_by(|a, b| a.total_cmp(b));
    let p95 = overheads[overheads.len() * 95 / 100];
    eprintln!("overhead p95 = {p95} ms (n = {})", overheads.len());
    assert!(p95 < 2.0, "p95 overhead {p95} ms");
}
