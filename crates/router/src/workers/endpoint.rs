use std::time::Instant;

#[derive(Debug, Clone)]
pub struct LoadSample {
    pub running: u64,
    pub waiting: u64,
    pub kv_usage: f64,
    pub collected_at: Instant,
}

#[derive(Debug, Clone)]
pub struct EndpointSnapshot {
    pub key: crate::types::WorkerKey,
    pub generation: u64,
    pub trust: Trust,
    pub load: Option<LoadSample>,
    pub reservations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trust {
    Warming,
    Trusted,
    Untrusted,
}
