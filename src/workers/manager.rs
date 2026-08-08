use crate::config::RouterConfig;
use crate::routing::load::{LoadError, parse_metrics};
use crate::types::{CandidateSet, WorkerKey};
use crate::workers::endpoint::{EndpointSnapshot, LoadSample, Trust};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

const METRICS_PORT: u16 = 8000;

#[derive(Debug)]
pub enum Completion {
    Released,
    Mismatch,
    Unknown,
}

struct EndpointState {
    key: WorkerKey,
    generation: u64,
    trust: Trust,
    last_load: Option<LoadSample>,
    reservations: HashMap<Uuid, Instant>,
    last_seen: Instant,
}

impl EndpointState {
    fn snapshot(&self) -> EndpointSnapshot {
        EndpointSnapshot {
            key: self.key.clone(),
            generation: self.generation,
            trust: self.trust.clone(),
            load: self.last_load.clone(),
            reservations: self.reservations.len(),
        }
    }
}

pub struct EndpointManager {
    states: Mutex<HashMap<WorkerKey, EndpointState>>,
    client: reqwest::Client,
    config: RouterConfig,
}

impl EndpointManager {
    pub fn new(client: reqwest::Client, config: RouterConfig) -> Self {
        EndpointManager {
            states: Mutex::new(HashMap::new()),
            client,
            config,
        }
    }

    pub async fn observe(&self, candidates: &CandidateSet, now: Instant) {
        let mut states = self.states.lock().await;
        for worker in &candidates.0 {
            states
                .entry(worker.clone())
                .or_insert_with(|| EndpointState {
                    key: worker.clone(),
                    generation: 1,
                    trust: Trust::Warming,
                    last_load: None,
                    reservations: HashMap::new(),
                    last_seen: now,
                })
                .last_seen = now;
        }
    }

    pub async fn snapshots(&self, candidates: &CandidateSet) -> Vec<EndpointSnapshot> {
        let states = self.states.lock().await;
        candidates
            .0
            .iter()
            .filter_map(|w| states.get(w).map(|s| s.snapshot()))
            .collect()
    }

    pub async fn reserve(&self, decision: Uuid, worker: WorkerKey, expires: Instant) {
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(&worker) {
            state.reservations.insert(decision, expires);
        }
    }

    pub async fn complete(&self, decision: Uuid, served: WorkerKey) -> Completion {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            if state.reservations.remove(&decision).is_some() {
                return if state.key == served {
                    Completion::Released
                } else {
                    Completion::Mismatch
                };
            }
        }
        Completion::Unknown
    }

    pub async fn reap(&self, now: Instant) {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            state.reservations.retain(|_, expires| *expires > now);
        }
        states.retain(|_, state| {
            now.duration_since(state.last_seen).as_secs() < self.config.endpoint_state_ttl_secs
        });
    }

    pub async fn fetch_load(&self, worker: &WorkerKey) -> Result<LoadSample, LoadError> {
        let text = self
            .client
            .get(format!("http://{}:{METRICS_PORT}/metrics", worker.0.ip()))
            .send()
            .await
            .map_err(|e| LoadError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| LoadError::Http(e.to_string()))?;
        let parsed = parse_metrics(&text)?;
        Ok(LoadSample {
            running: parsed.running,
            waiting: parsed.waiting,
            kv_usage: parsed.kv_usage,
            collected_at: Instant::now(),
        })
    }

    pub async fn update_load(&self, worker: &WorkerKey, now: Instant) {
        if let Ok(sample) = self.fetch_load(worker).await {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(worker) {
                state.last_load = Some(sample);
                state.last_seen = now;
            }
        }
    }
}
