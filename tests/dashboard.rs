//! T25: the embedded dashboard has no external URLs, is small, and is served at `/`.

use std::collections::HashMap;

use sluice::AppState;
use sluice::config::Config;
use tokio::net::TcpListener;

#[test]
fn t25_dashboard_file_has_no_external_urls_and_is_small() {
    let page = sluice::status::DASHBOARD_HTML;
    assert!(!page.contains("http://"), "dashboard contains http://");
    assert!(!page.contains("https://"), "dashboard contains https://");
    assert!(page.len() < 60 * 1024, "dashboard is {} bytes", page.len());
}

#[tokio::test]
async fn t25_root_serves_the_dashboard_as_html() {
    let env: HashMap<&str, &str> = [("MOCK_KEYS", "mock-key-00000001")].into();
    let config = Config::from_toml_str(
        "[[provider]]\nid = \"mock\"\nbase_url = \"http://127.0.0.1:9/v1\"\nkeys_env = \"MOCK_KEYS\"\n",
        &|k| env.get(k).map(|v| v.to_string()),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(config).unwrap();
    tokio::spawn(sluice::serve(listener, state, std::future::pending()));

    let r = sluice::proxy::build_client()
        .unwrap()
        .get(format!("http://{addr}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let ct = r.headers()["content-type"].to_str().unwrap().to_string();
    assert!(ct.starts_with("text/html"), "content-type {ct}");
    assert_eq!(r.text().await.unwrap(), sluice::status::DASHBOARD_HTML);
}
