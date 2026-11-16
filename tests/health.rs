//! Starts the server in-process on an ephemeral port and talks raw HTTP/1.1
//! to it (no HTTP client crate until M1).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sluice::AppState;
use sluice::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const PROVIDER: &str = r#"
[[provider]]
id = "mock"
base_url = "http://127.0.0.1:9/v1"
keys_env = "MOCK_KEYS"
"#;

const CLIENT_TOKEN: &str = "client-token-for-tests-1234";

fn config(bind: &str) -> Config {
    let env: HashMap<&str, &str> = [
        ("MOCK_KEYS", "mock-key-00000001"),
        ("ROUTER_API_KEY", CLIENT_TOKEN),
    ]
    .into();
    let text = format!("bind = \"{bind}\"\n{PROVIDER}");
    Config::from_toml_str(&text, &|k| env.get(k).map(|v| v.to_string()))
        .unwrap_or_else(|e| panic!("test config invalid: {e}"))
}

struct Server {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Server {
    /// Always listens on 127.0.0.1:0; `config.bind` only drives auth policy.
    async fn start(config: Config) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let state: Arc<AppState> = AppState::new(config);
        let task = tokio::spawn(sluice::serve(listener, state, async {
            let _ = rx.await;
        }));
        Server {
            addr,
            stop: Some(tx),
            task,
        }
    }

    async fn get(&self, path: &str, auth: Option<&str>) -> (u16, String) {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();
        let mut req = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.addr
        );
        if let Some(a) = auth {
            req.push_str(&format!("Authorization: {a}\r\n"));
        }
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
            .await
            .expect("response timed out")
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        let status = text
            .strip_prefix("HTTP/1.1 ")
            .and_then(|s| s.get(..3))
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("bad status line in {text:?}"));
        (status, text)
    }

    async fn stop(mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("server did not shut down")
            .unwrap()
            .unwrap();
    }
}

fn body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or("")
}

#[tokio::test]
async fn health_returns_200() {
    let server = Server::start(config("127.0.0.1:0")).await;
    let (status, text) = server.get("/health", None).await;
    assert_eq!(status, 200, "{text}");
    assert!(body(&text).contains("\"ok\""), "{text}");
    server.stop().await;
}

#[tokio::test]
async fn unknown_path_is_openai_shaped_404() {
    let server = Server::start(config("127.0.0.1:0")).await;
    let (status, text) = server.get("/nope", None).await;
    assert_eq!(status, 404, "{text}");
    let v: serde_json::Value = serde_json::from_str(body(&text)).unwrap();
    assert_eq!(v["error"]["code"], "not_found");
    assert_eq!(v["error"]["type"], "invalid_request_error");
    server.stop().await;
}

#[tokio::test]
async fn loopback_bind_needs_no_token() {
    let server = Server::start(config("127.0.0.1:0")).await;
    let (status, _) = server.get("/health", Some("Bearer wrong")).await;
    assert_eq!(status, 200);
    server.stop().await;
}

#[tokio::test]
async fn non_loopback_bind_requires_bearer_on_every_path() {
    let server = Server::start(config("0.0.0.0:0")).await;

    let (status, text) = server.get("/health", None).await;
    assert_eq!(status, 401, "{text}");
    assert!(!text.contains(CLIENT_TOKEN));
    let v: serde_json::Value = serde_json::from_str(body(&text)).unwrap();
    assert_eq!(v["error"]["code"], "invalid_api_key");

    let (status, _) = server.get("/health", Some("Bearer not-the-token")).await;
    assert_eq!(status, 401);

    let (status, _) = server.get("/nope", None).await;
    assert_eq!(status, 401);

    let (status, text) = server
        .get("/health", Some(&format!("Bearer {CLIENT_TOKEN}")))
        .await;
    assert_eq!(status, 200, "{text}");

    let (status, _) = server
        .get("/nope", Some(&format!("Bearer {CLIENT_TOKEN}")))
        .await;
    assert_eq!(status, 404);

    server.stop().await;
}
