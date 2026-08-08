use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("missing mini_router envelope")]
    MissingEnvelope,
    #[error("invalid envelope version: {0}")]
    InvalidVersion(i32),
    #[error("empty token_ids")]
    EmptyTokens,
    #[error("too many tokens")]
    TooManyTokens,
    #[error("invalid envelope format: {0}")]
    InvalidFormat(String),
    #[error("duplicate mini_router key rejected")]
    DuplicateKey,
}

#[derive(Deserialize)]
struct Envelope {
    version: i32,
    #[allow(dead_code)]
    decision_id: String,
    token_ids: Vec<u32>,
}

fn duplicated(haystack: &[u8], key: &[u8]) -> bool {
    match haystack.windows(key.len()).position(|w| w == key) {
        Some(i) => haystack[i + key.len()..]
            .windows(key.len())
            .any(|w| w == key),
        None => false,
    }
}

pub fn transform(body: &[u8], limit: usize) -> Result<Value, TransformError> {
    if duplicated(body, b"\"mini_router\"") || duplicated(body, b"\"kv_transfer_params\"") {
        return Err(TransformError::DuplicateKey);
    }

    let mut body: Value =
        serde_json::from_slice(body).map_err(|e| TransformError::InvalidFormat(e.to_string()))?;

    let envelope: Envelope = serde_json::from_value(
        body.get("mini_router")
            .ok_or(TransformError::MissingEnvelope)?
            .clone(),
    )
    .map_err(|e| TransformError::InvalidFormat(e.to_string()))?;

    if envelope.version != 1 {
        return Err(TransformError::InvalidVersion(envelope.version));
    }
    if envelope.token_ids.is_empty() {
        return Err(TransformError::EmptyTokens);
    }
    if envelope.token_ids.len() > limit {
        return Err(TransformError::TooManyTokens);
    }

    if let Some(obj) = body.as_object_mut() {
        obj.remove("mini_router");
        obj.remove("kv_transfer_params");
        obj.insert(
            "kv_transfer_params".into(),
            serde_json::json!({
                "prompt_token_ids": envelope.token_ids,
            }),
        );
    }
    Ok(body)
}
