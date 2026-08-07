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
struct MiniRouterEnvelope {
    version: i32,
    #[allow(dead_code)]
    decision_id: String,
    token_ids: Vec<u32>,
}

fn count_key_occurrences(haystack: &[u8], key: &[u8]) -> usize {
    let mut count = 0;
    let mut pos = 0;
    while let Some(idx) = haystack[pos..].windows(key.len()).position(|w| w == key) {
        count += 1;
        pos += idx + key.len();
        if count > 1 {
            return count;
        }
    }
    count
}

pub fn transform(raw_body: &[u8], max_tokens: usize) -> Result<Value, TransformError> {
    if count_key_occurrences(raw_body, b"\"mini_router\"") > 1 {
        return Err(TransformError::DuplicateKey);
    }
    if count_key_occurrences(raw_body, b"\"kv_transfer_params\"") > 1 {
        return Err(TransformError::DuplicateKey);
    }

    let body: Value = serde_json::from_slice(raw_body)
        .map_err(|e| TransformError::InvalidFormat(e.to_string()))?;

    let envelope: MiniRouterEnvelope = body
        .get("mini_router")
        .ok_or(TransformError::MissingEnvelope)
        .and_then(|v| {
            serde_json::from_value(v.clone())
                .map_err(|e| TransformError::InvalidFormat(e.to_string()))
        })?;

    if envelope.version != 1 {
        return Err(TransformError::InvalidVersion(envelope.version));
    }

    if envelope.token_ids.is_empty() {
        return Err(TransformError::EmptyTokens);
    }

    if envelope.token_ids.len() > max_tokens {
        return Err(TransformError::TooManyTokens);
    }

    let mut sanitized = body;
    if let Some(obj) = sanitized.as_object_mut() {
        obj.remove("mini_router");
        obj.remove("kv_transfer_params");
        obj.insert(
            "kv_transfer_params".to_string(),
            serde_json::json!({
                "prompt_token_ids": envelope.token_ids,
            }),
        );
    }

    Ok(sanitized)
}
