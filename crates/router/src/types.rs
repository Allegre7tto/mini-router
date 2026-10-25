use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerKey(pub SocketAddr);

impl WorkerKey {
    pub fn metrics(&self, port: u16) -> WorkerKey {
        WorkerKey(SocketAddr::new(self.0.ip(), port))
    }
}

impl fmt::Display for WorkerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WorkerKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<SocketAddr>()
            .map(WorkerKey)
            .map_err(|e| format!("invalid endpoint: {e}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandidateSet(pub BTreeSet<WorkerKey>);

impl CandidateSet {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct RenderedInput {
    pub token_ids: std::sync::Arc<[u32]>,
}

#[derive(Debug, Clone)]
pub struct RendererIdentity {
    pub model: String,
    pub revision: String,
    pub probe_digest: String,
}

#[derive(Debug, Clone)]
pub struct RoutingRequest {
    pub request_id: String,
    pub body: serde_json::Value,
    pub candidates: CandidateSet,
}

#[derive(Debug, Clone)]
pub struct RoutingDecision {
    pub decision_id: uuid::Uuid,
    pub endpoint: WorkerKey,
    pub generation: u64,
    pub matched_tokens: usize,
    pub effective_load: f64,
    pub body: Vec<u8>,
}
