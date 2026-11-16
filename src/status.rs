//! Operational endpoints. M0 provides only `GET /health`.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::get;
use serde_json::{Value, json};

use crate::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/health", get(health))
}

/// Returns 200 whenever the process is up.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
