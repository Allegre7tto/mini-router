use router::adapter::transform::{TransformError, transform};

fn raw(body: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(body).unwrap()
}

#[test]
fn transform_injects_prompt_token_ids() {
    let body = serde_json::json!({
        "model": "fixed",
        "messages": [{"role": "user", "content": "hi"}],
        "mini_router": {
            "version": 1,
            "decision_id": "uuid-1",
            "token_ids": [1, 2, 3]
        }
    });
    let result = transform(&raw(&body), 128000).unwrap();
    assert!(result.get("mini_router").is_none());
    let kvt = result.get("kv_transfer_params").unwrap();
    assert_eq!(kvt["prompt_token_ids"].as_array().unwrap().len(), 3);
    assert_eq!(result["model"], "fixed");
}

#[test]
fn transform_rejects_wrong_version() {
    let body = serde_json::json!({
        "model": "fixed",
        "messages": [{"role": "user", "content": "hi"}],
        "mini_router": {"version": 2, "decision_id": "u", "token_ids": [1]}
    });
    let result = transform(&raw(&body), 128000);
    assert!(matches!(result, Err(TransformError::InvalidVersion(2))));
}

#[test]
fn transform_rejects_empty_tokens() {
    let body = serde_json::json!({
        "model": "fixed",
        "messages": [{"role": "user", "content": "hi"}],
        "mini_router": {"version": 1, "decision_id": "u", "token_ids": []}
    });
    let result = transform(&raw(&body), 128000);
    assert!(matches!(result, Err(TransformError::EmptyTokens)));
}

#[test]
fn transform_rejects_over_limit() {
    let body = serde_json::json!({
        "model": "fixed",
        "messages": [{"role": "user", "content": "hi"}],
        "mini_router": {"version": 1, "decision_id": "u", "token_ids": [1,2,3]}
    });
    let result = transform(&raw(&body), 2);
    assert!(matches!(result, Err(TransformError::TooManyTokens)));
}

#[test]
fn transform_removes_client_kv_transfer_params() {
    let body = serde_json::json!({
        "model": "fixed",
        "messages": [{"role": "user", "content": "hi"}],
        "mini_router": {"version": 1, "decision_id": "u", "token_ids": [1]},
        "kv_transfer_params": {"prompt_token_ids": [999]}
    });
    let result = transform(&raw(&body), 128000).unwrap();
    let kvt = result.get("kv_transfer_params").unwrap();
    assert_eq!(kvt["prompt_token_ids"][0], 1);
}

#[test]
fn transform_rejects_missing_envelope() {
    let body =
        serde_json::json!({"model": "fixed", "messages": [{"role": "user", "content": "hi"}]});
    let result = transform(&raw(&body), 128000);
    assert!(matches!(result, Err(TransformError::MissingEnvelope)));
}

#[test]
fn duplicate_keys_rejected() {
    let raw = b"{\"model\":\"fixed\",\"mini_router\":{\"version\":1,\"decision_id\":\"u\",\"token_ids\":[1]},\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"mini_router\":{\"version\":1,\"decision_id\":\"u\",\"token_ids\":[2]}}";
    let result = transform(raw, 128000);
    assert!(result.is_err());
}
