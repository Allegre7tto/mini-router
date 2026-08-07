use crate::config::RouterConfig;
use crate::kv::index::PrefixIndex;
use crate::renderer::client::Renderer;
use crate::renderer::request::validate;
use crate::routing::scorer;
use crate::types::{RoutingDecision, RoutingRequest, WorkerKey};
use crate::workers::manager::{Completion, EndpointManager};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("validation error: {0}")]
    Validation(#[from] crate::renderer::request::RequestError),
    #[error("render error: {0}")]
    Render(#[from] crate::renderer::client::RenderError),
    #[error("routing error: {0}")]
    Routing(#[from] scorer::RoutingError),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

pub struct RoutingService {
    renderer: Box<dyn Renderer>,
    index: Arc<tokio::sync::Mutex<PrefixIndex>>,
    manager: Arc<EndpointManager>,
    config: RouterConfig,
}

impl RoutingService {
    pub fn new(
        renderer: Box<dyn Renderer>,
        index: Arc<tokio::sync::Mutex<PrefixIndex>>,
        manager: Arc<EndpointManager>,
        config: RouterConfig,
    ) -> Self {
        RoutingService {
            renderer,
            index,
            manager,
            config,
        }
    }

    pub async fn route(&self, request: RoutingRequest) -> Result<RoutingDecision, ServiceError> {
        let sanitized = validate(&request.body, &self.config)?;

        let now = Instant::now();
        self.manager.observe(&request.candidates, now).await;

        let rendered = self.renderer.render(&sanitized).await?;

        let candidates = &request.candidates;
        let snapshots = self.manager.snapshots(candidates).await;

        let generations: std::collections::BTreeMap<WorkerKey, u64> = snapshots
            .iter()
            .map(|s| (s.key.clone(), s.generation))
            .collect();
        let matches = {
            let index = self.index.lock().await;
            index.match_prefix(&rendered.token_ids, candidates, &generations)
        };

        let scored = scorer::choose(
            &request.request_id,
            rendered.token_ids.len(),
            candidates,
            &matches,
            &snapshots,
            &self.config,
        )?;

        let decision_id = Uuid::new_v4();
        let expiry = now + std::time::Duration::from_secs(self.config.reservation_ttl_secs);
        self.manager
            .reserve(decision_id, scored.endpoint.clone(), expiry)
            .await;

        let mut body = sanitized;
        let token_ids_vec: Vec<u32> = rendered.token_ids.iter().copied().collect();
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "mini_router".to_string(),
                serde_json::json!({
                    "version": 1,
                    "decision_id": decision_id.to_string(),
                    "token_ids": token_ids_vec,
                }),
            );
        }
        let body_bytes = serde_json::to_vec(&body)?;

        Ok(RoutingDecision {
            decision_id,
            endpoint: scored.endpoint,
            generation: scored.generation,
            matched_tokens: scored.matched_tokens,
            effective_load: scored.effective_load,
            body: body_bytes,
        })
    }

    pub async fn complete_served(&self, decision_id: Uuid, served: WorkerKey) -> Completion {
        self.manager.complete(decision_id, served).await
    }
}
