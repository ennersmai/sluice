//! Error type for HTTP handlers, rendered as OpenAI-shaped JSON bodies:
//! `{"error": {"message": ..., "type": ..., "param": null, "code": ...}}`.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub kind: &'static str,
    pub code: &'static str,
}

impl ApiError {
    pub fn new(
        status: StatusCode,
        kind: &'static str,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            kind,
            code,
        }
    }

    /// Missing or wrong bearer token.
    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "invalid_api_key",
            "missing or invalid bearer token",
        )
    }

    /// Unknown path.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            "not_found",
            message,
        )
    }

    pub fn body(&self) -> serde_json::Value {
        json!({
            "error": {
                "message": self.message,
                "type": self.kind,
                "param": null,
                "code": self.code,
            }
        })
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}: {}",
            self.status.as_u16(),
            self.code,
            self.message
        )
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body())).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_is_openai_shaped() {
        let e = ApiError::not_found("no such thing");
        assert_eq!(
            e.body(),
            json!({"error": {"message": "no such thing", "type": "invalid_request_error", "param": null, "code": "not_found"}})
        );
    }

    #[test]
    fn unauthorized_sets_www_authenticate() {
        let r = ApiError::unauthorized().into_response();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(r.headers()[header::WWW_AUTHENTICATE], "Bearer");
    }
}
