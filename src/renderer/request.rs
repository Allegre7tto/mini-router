use crate::config::RouterConfig;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("body too large")]
    BodyTooLarge,
    #[error("model mismatch: expected configured model")]
    ModelMismatch,
    #[error("messages must not be empty")]
    EmptyMessages,
    #[error("invalid message role: {0}")]
    InvalidRole(String),
    #[error("invalid content: expected string")]
    InvalidContent,
    #[error("serialization failed")]
    Serialize,
}

#[derive(Deserialize)]
struct Chat {
    model: String,
    messages: Vec<Message>,
}

#[derive(Deserialize)]
struct Message {
    role: String,
    #[allow(dead_code)]
    content: String,
}

pub fn validate(
    body: &serde_json::Value,
    config: &RouterConfig,
) -> Result<serde_json::Value, RequestError> {
    let body_bytes = serde_json::to_vec(body).map_err(|_| RequestError::Serialize)?;
    if body_bytes.len() > config.max_body_bytes {
        return Err(RequestError::BodyTooLarge);
    }

    let chat: Chat =
        serde_json::from_value(body.clone()).map_err(|_| RequestError::InvalidContent)?;

    if chat.model != config.model {
        return Err(RequestError::ModelMismatch);
    }

    if chat.messages.is_empty() {
        return Err(RequestError::EmptyMessages);
    }

    for msg in &chat.messages {
        if !matches!(
            msg.role.as_str(),
            "system" | "user" | "assistant" | "developer"
        ) {
            return Err(RequestError::InvalidRole(msg.role.clone()));
        }
    }

    let mut sanitized = body.clone();
    if let Some(obj) = sanitized.as_object_mut() {
        obj.remove("mini_router");
        obj.remove("kv_transfer_params");
    }
    Ok(sanitized)
}
