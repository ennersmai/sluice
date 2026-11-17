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
    /// Request field the error refers to, rendered as `param`.
    pub param: Option<&'static str>,
    /// Close matches for an unknown model, rendered as `suggestions`.
    pub suggestions: Option<Vec<String>>,
    /// Per-route skip reasons for a 503, rendered as `routes`.
    pub routes: Option<Box<serde_json::Value>>,
    /// Seconds for a `Retry-After` response header.
    pub retry_after: Option<u32>,
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
            param: None,
            suggestions: None,
            routes: None,
            retry_after: None,
        }
    }

    pub fn with_param(mut self, param: &'static str) -> Self {
        self.param = Some(param);
        self
    }

    pub fn with_suggestions(mut self, suggestions: Vec<String>) -> Self {
        self.suggestions = Some(suggestions);
        self
    }

    /// Malformed client request.
    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            code,
            message,
        )
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

    pub fn with_routes(mut self, routes: serde_json::Value) -> Self {
        self.routes = Some(Box::new(routes));
        self
    }

    pub fn with_retry_after(mut self, secs: u32) -> Self {
        self.retry_after = Some(secs);
        self
    }

    pub fn body(&self) -> serde_json::Value {
        let mut error = json!({
            "message": self.message,
            "type": self.kind,
            "param": self.param,
            "code": self.code,
        });
        if let (Some(routes), Some(obj)) = (&self.routes, error.as_object_mut()) {
            obj.insert("routes".to_string(), (**routes).clone());
        }
        if let (Some(list), Some(obj)) = (&self.suggestions, error.as_object_mut()) {
            obj.insert("suggestions".to_string(), json!(list));
        }
        json!({ "error": error })
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
        if let Some(secs) = self.retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
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
    fn param_and_suggestions_are_rendered() {
        let e = ApiError::not_found("unknown model")
            .with_param("model")
            .with_suggestions(vec!["coder".to_string()]);
        assert_eq!(
            e.body(),
            json!({"error": {"message": "unknown model", "type": "invalid_request_error", "param": "model", "code": "not_found", "suggestions": ["coder"]}})
        );
    }

    #[test]
    fn unauthorized_sets_www_authenticate() {
        let r = ApiError::unauthorized().into_response();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(r.headers()[header::WWW_AUTHENTICATE], "Bearer");
    }
}
