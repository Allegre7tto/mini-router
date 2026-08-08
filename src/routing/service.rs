use crate::config::RouterConfig;
use crate::kv::index::PrefixIndex;
use crate::renderer::client::Renderer;
use crate::renderer::request::validate;
use crate::routing::scorer;
use crate::types::{RoutingDecision, RoutingRequest, WorkerKey};
use crate::workers::manager::{Completion, EndpointManager};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
        let mut body = validate(&request.body, &self.config)?;

        let now = Instant::now();
        self.manager.observe(&request.candidates, now).await;

        let rendered = self.renderer.render(&body).await?;

        let snapshots = self.manager.snapshots(&request.candidates).await;
        let generations: BTreeMap<WorkerKey, u64> = snapshots
            .iter()
            .map(|s| (s.key.clone(), s.generation))
            .collect();
        let matches = self.index.lock().await.match_prefix(
            &rendered.token_ids,
            &request.candidates,
            &generations,
        );

        let scored = scorer::choose(
            &request.request_id,
            rendered.token_ids.len(),
            &request.candidates,
            &matches,
            &snapshots,
            &self.config,
        )?;

        let decision = Uuid::new_v4();
        let expiry = now + Duration::from_secs(self.config.reservation_ttl_secs);
        self.manager
            .reserve(decision, scored.endpoint.clone(), expiry)
            .await;

        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "mini_router".into(),
                serde_json::json!({
                    "version": 1,
                    "decision_id": decision.to_string(),
                    "token_ids": rendered.token_ids.to_vec(),
                }),
            );
        }

        Ok(RoutingDecision {
            decision_id: decision,
            endpoint: scored.endpoint,
            generation: scored.generation,
            matched_tokens: scored.matched_tokens,
            effective_load: scored.effective_load,
            body: serde_json::to_vec(&body)?,
        })
    }

    pub async fn complete_served(&self, decision: Uuid, served: WorkerKey) -> Completion {
        self.manager.complete(decision, served).await
    }
}
