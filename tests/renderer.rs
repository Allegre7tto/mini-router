use router::config::RouterConfig;
use router::renderer::client::{HttpRenderer, RenderError, Renderer};
use router::renderer::request::{RequestError, validate};
use router::types::RendererIdentity;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn render_config() -> RouterConfig {
    RouterConfig::fixture()
}

#[test]
fn validate_rejects_wrong_model() {
    let config = render_config();
    let body = json!({"model":"other","messages":[{"role":"user","content":"hi"}]});
    let result = validate(&body, &config);
    assert!(matches!(result, Err(RequestError::ModelMismatch)));
}

#[test]
fn validate_rejects_nonstring_content() {
    let config = render_config();
    let body = json!({"model":"fixed","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]});
    let result = validate(&body, &config);
    assert!(result.is_err());
}

#[test]
fn validate_removes_reserved_and_rejects_empty() {
    let config = render_config();
    let body = json!({"model":"fixed","messages":[],"mini_router":{"token_ids":[1]}});
    let result = validate(&body, &config);
    assert!(matches!(result, Err(RequestError::EmptyMessages)));
}

#[test]
fn validate_accepts_and_sanitizes() {
    let config = render_config();
    let body = json!({
        "model": "fixed",
        "messages": [{"role":"user","content":"hi"}],
        "mini_router": {"token_ids": [1]},
        "kv_transfer_params": {"prompt_token_ids": [1]}
    });
    let sanitized = validate(&body, &config).unwrap();
    assert!(sanitized.get("mini_router").is_none());
    assert!(sanitized.get("kv_transfer_params").is_none());
    assert_eq!(sanitized["model"], "fixed");
}

#[test]
fn validate_rejects_body_too_large() {
    let mut config = render_config();
    config.max_body_bytes = 10;
    let body = json!({"model":"fixed","messages":[{"role":"user","content":"hi there"}]});
    let result = validate(&body, &config);
    assert!(matches!(result, Err(RequestError::BodyTooLarge)));
}

#[test]
fn validate_rejects_invalid_role() {
    let config = render_config();
    let body = json!({"model":"fixed","messages":[{"role":"tool","content":"hi"}]});
    let result = validate(&body, &config);
    assert!(matches!(result, Err(RequestError::InvalidRole(_))));
}

#[tokio::test]
async fn renderer_parses_token_ids() {
    let server = MockServer::start().await;
    let fixture = include_str!("fixtures/renders/chat.json");

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions/render"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(fixture, "application/json"))
        .mount(&server)
        .await;

    let renderer = HttpRenderer::new(server.uri(), std::time::Duration::from_secs(30), 128000);
    let body = json!({"model":"fixed","messages":[{"role":"user","content":"hi"}]});
    let result = renderer.render(&body).await.unwrap();
    assert_eq!(result.token_ids.as_ref(), &[1, 2, 3, 4]);
}

#[tokio::test]
async fn renderer_rejects_empty_token_ids() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions/render"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"request_id":"r1","token_ids":[],"sampling_params":{}}),
        ))
        .mount(&server)
        .await;

    let renderer = HttpRenderer::new(server.uri(), std::time::Duration::from_secs(30), 128000);
    let body = json!({"model":"fixed","messages":[{"role":"user","content":"hi"}]});
    let result = renderer.render(&body).await;
    assert!(matches!(result, Err(RenderError::EmptyTokens)));
}

#[tokio::test]
async fn renderer_rejects_over_limit_tokens() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions/render"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"request_id":"r1","token_ids":[1,2,3],"sampling_params":{}}),
        ))
        .mount(&server)
        .await;

    let renderer = HttpRenderer::new(server.uri(), std::time::Duration::from_secs(30), 2);
    let body = json!({"model":"fixed","messages":[{"role":"user","content":"hi"}]});
    let result = renderer.render(&body).await;
    assert!(matches!(result, Err(RenderError::TooManyTokens)));
}

#[tokio::test]
async fn renderer_ready_checks_model() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"object":"list","data":[{"id":"other","root":"other","owned_by":""}]}),
        ))
        .mount(&server)
        .await;

    let renderer = HttpRenderer::new(server.uri(), std::time::Duration::from_secs(30), 128000);
    let result = renderer
        .ready(&RendererIdentity {
            model: "fixed".into(),
            revision: "abc123".into(),
            probe_digest: RouterConfig::fixture().probe_digest,
        })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn renderer_ready_checks_identity() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"object":"list","data":[{"id":"fixed","root":"abc123","owned_by":""}]}),
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions/render"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"request_id":"r1","token_ids":[1,2,3,4],"sampling_params":{}}),
        ))
        .mount(&server)
        .await;

    let renderer = HttpRenderer::new(server.uri(), std::time::Duration::from_secs(30), 128000);
    assert!(
        renderer
            .ready(&RendererIdentity {
                model: "fixed".into(),
                revision: "abc123".into(),
                probe_digest: RouterConfig::fixture().probe_digest,
            })
            .await
            .is_ok()
    );
}
