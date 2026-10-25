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
struct Chat<'a> {
    model: &'a str,
    messages: Vec<Message<'a>>,
}

#[derive(Deserialize)]
struct Message<'a> {
    role: &'a str,
    #[allow(dead_code)]
    content: &'a str,
}

pub fn validate(
    body: &serde_json::Value,
    config: &RouterConfig,
) -> Result<serde_json::Value, RequestError> {
    if serde_json::to_vec(body)
        .map_err(|_| RequestError::Serialize)?
        .len()
        > config.max_body_bytes
    {
        return Err(RequestError::BodyTooLarge);
    }

    let chat = Chat::deserialize(body).map_err(|_| RequestError::InvalidContent)?;
    if chat.model != config.model {
        return Err(RequestError::ModelMismatch);
    }
    if chat.messages.is_empty() {
        return Err(RequestError::EmptyMessages);
    }
    for msg in &chat.messages {
        if !matches!(msg.role, "system" | "user" | "assistant" | "developer") {
            return Err(RequestError::InvalidRole(msg.role.into()));
        }
    }

    let mut body = body.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.remove("mini_router");
        obj.remove("kv_transfer_params");
    }
    Ok(body)
}
