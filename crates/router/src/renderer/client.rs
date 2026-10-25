use crate::types::{RenderedInput, RendererIdentity};
use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("http error: {0}")]
    Http(String),
    #[error("empty token_ids in response")]
    EmptyTokens,
    #[error("too many tokens")]
    TooManyTokens,
    #[error("model not found in /v1/models")]
    ModelNotFound,
    #[error("renderer identity mismatch")]
    IdentityMismatch,
}

#[derive(Deserialize)]
struct RenderResponse {
    token_ids: Vec<u32>,
}

#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<ModelEntry>,
}

#[derive(Deserialize)]
struct ModelEntry {
    id: String,
    #[allow(dead_code)]
    root: String,
}

#[async_trait]
pub trait Renderer: Send + Sync {
    async fn render(&self, body: &serde_json::Value) -> Result<RenderedInput, RenderError>;
    async fn ready(&self, identity: &RendererIdentity) -> Result<(), RenderError>;
}

pub struct HttpRenderer {
    client: reqwest::Client,
    base_url: String,
    max_token_count: usize,
}

impl HttpRenderer {
    pub fn new(base_url: String, timeout: Duration, max_token_count: usize) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build reqwest client");
        HttpRenderer {
            client,
            base_url,
            max_token_count,
        }
    }
}

#[async_trait]
impl Renderer for HttpRenderer {
    async fn render(&self, body: &serde_json::Value) -> Result<RenderedInput, RenderError> {
        let resp = self
            .client
            .post(format!("{}/v1/chat/completions/render", self.base_url))
            .json(body)
            .send()
            .await
            .map_err(|e| RenderError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(RenderError::Http(format!(
                "render returned status {}",
                resp.status()
            )));
        }

        let rendered: RenderResponse = resp
            .json()
            .await
            .map_err(|e| RenderError::Http(e.to_string()))?;

        if rendered.token_ids.is_empty() {
            return Err(RenderError::EmptyTokens);
        }
        if rendered.token_ids.len() > self.max_token_count {
            return Err(RenderError::TooManyTokens);
        }

        Ok(RenderedInput {
            token_ids: rendered.token_ids.into(),
        })
    }

    async fn ready(&self, identity: &RendererIdentity) -> Result<(), RenderError> {
        let models: ModelsResponse = self
            .client
            .get(format!("{}/v1/models", self.base_url))
            .send()
            .await
            .map_err(|e| RenderError::Http(e.to_string()))?
            .json()
            .await
            .map_err(|e| RenderError::Http(e.to_string()))?;

        if !models
            .data
            .iter()
            .any(|m| m.id == identity.model && m.root == identity.revision)
        {
            return Err(RenderError::ModelNotFound);
        }

        let tokens = self
            .render(&serde_json::json!({
                "model": identity.model,
                "messages": [{"role": "user", "content": "mini-router identity probe"}]
            }))
            .await?;
        let mut hash = Sha256::new();
        for token in tokens.token_ids.iter() {
            hash.update(token.to_be_bytes());
        }
        if format!("sha256:{:x}", hash.finalize()) != identity.probe_digest {
            return Err(RenderError::IdentityMismatch);
        }
        Ok(())
    }
}
