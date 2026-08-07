use crate::adapter::transform::transform;
use crate::config::AdapterConfig;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
};
use std::sync::Arc;

pub struct AdapterState {
    pub config: AdapterConfig,
    pub client: reqwest::Client,
}

pub fn router(state: Arc<AdapterState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/chat/completions", post(chat))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics() -> &'static str {
    ""
}

async fn chat(
    State(state): State<Arc<AdapterState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response<Body>, StatusCode> {
    let auth = state.config.workload_token.as_str();
    let header_auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    use subtle::ConstantTimeEq;
    if auth.as_bytes().ct_ne(header_auth.as_bytes()).into() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    if body.len() > state.config.max_body_bytes {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let has_envelope = body
        .windows(b"\"mini_router\"".len())
        .any(|w| w == b"\"mini_router\"");

    let upstream_body = if has_envelope {
        transform(&body, state.config.max_token_count).map_err(|_| StatusCode::BAD_REQUEST)?
    } else {
        let mut b: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        if let Some(obj) = b.as_object_mut() {
            obj.remove("mini_router");
            obj.remove("kv_transfer_params");
        }
        b
    };

    let resp = state
        .client
        .post(format!("{}/v1/chat/completions", state.config.vllm_url))
        .json(&upstream_body)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let status = resp.status();
    let headers = resp.headers().clone();
    let stream = resp.bytes_stream();

    let body = Body::from_stream(stream);

    let mut response = Response::builder().status(status);
    for (k, v) in headers.iter() {
        if k != "transfer-encoding" && k != "connection" {
            response = response.header(k, v);
        }
    }
    Ok(response.body(body).unwrap())
}
