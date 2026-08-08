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
use subtle::ConstantTimeEq;

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
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if state
        .config
        .workload_token
        .as_bytes()
        .ct_ne(auth.as_bytes())
        .into()
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    if body.len() > state.config.max_body_bytes {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let upstream = if body
        .windows(b"\"mini_router\"".len())
        .any(|w| w == b"\"mini_router\"")
    {
        transform(&body, state.config.max_token_count).map_err(|_| StatusCode::BAD_REQUEST)?
    } else {
        let mut body: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        if let Some(obj) = body.as_object_mut() {
            obj.remove("mini_router");
            obj.remove("kv_transfer_params");
        }
        body
    };

    let resp = state
        .client
        .post(format!("{}/v1/chat/completions", state.config.vllm_url))
        .json(&upstream)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let headers = resp.headers().clone();
    let mut builder = Response::builder().status(resp.status());
    for (k, v) in &headers {
        if k != "transfer-encoding" && k != "connection" {
            builder = builder.header(k, v);
        }
    }
    Ok(builder
        .body(Body::from_stream(resp.bytes_stream()))
        .unwrap())
}
