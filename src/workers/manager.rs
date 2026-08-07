use crate::config::RouterConfig;
use crate::routing::load::parse_metrics;
use crate::types::{CandidateSet, WorkerKey};
use crate::workers::endpoint::{EndpointSnapshot, LoadSample, Trust};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug)]
pub enum Completion {
    Released,
    Mismatch,
    Unknown,
}

#[derive(Debug)]
struct Reservation {
    #[allow(dead_code)]
    decision_id: Uuid,
    #[allow(dead_code)]
    worker: WorkerKey,
    expires: Instant,
}

struct EndpointState {
    key: WorkerKey,
    generation: u64,
    trust: Trust,
    last_load: Option<LoadSample>,
    reservations: HashMap<Uuid, Reservation>,
    last_seen: Instant,
    #[allow(dead_code)]
    metric_port: u16,
    #[allow(dead_code)]
    kv_port: u16,
    #[allow(dead_code)]
    replay_port: u16,
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
            let entry = states
                .entry(worker.clone())
                .or_insert_with(|| EndpointState {
                    key: worker.clone(),
                    generation: 1,
                    trust: Trust::Warming,
                    last_load: None,
                    reservations: HashMap::new(),
                    last_seen: now,
                    metric_port: 8000,
                    kv_port: 5557,
                    replay_port: 5558,
                });
            entry.last_seen = now;
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

    pub async fn reserve(&self, decision_id: Uuid, worker: WorkerKey, expires: Instant) {
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(&worker) {
            state.reservations.insert(
                decision_id,
                Reservation {
                    decision_id,
                    worker: worker.clone(),
                    expires,
                },
            );
        }
    }

    pub async fn complete(&self, decision_id: Uuid, served: WorkerKey) -> Completion {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            if state.reservations.remove(&decision_id).is_some() {
                if state.key == served {
                    return Completion::Released;
                } else {
                    return Completion::Mismatch;
                }
            }
        }
        Completion::Unknown
    }

    pub async fn reap(&self, now: Instant) {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            state.reservations.retain(|_, r| r.expires > now);
        }
        states.retain(|_, state| {
            now.duration_since(state.last_seen).as_secs() < self.config.endpoint_state_ttl_secs
        });
    }

    pub async fn fetch_load(
        &self,
        worker: &WorkerKey,
    ) -> Result<LoadSample, crate::routing::load::LoadError> {
        let url = format!("http://{}:{}/metrics", worker.0.ip(), 8000);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| crate::routing::load::LoadError::Http(e.to_string()))?;
        let text = resp
            .text()
            .await
            .map_err(|e| crate::routing::load::LoadError::Http(e.to_string()))?;
        let metrics = parse_metrics(&text)?;
        Ok(LoadSample {
            running: metrics.running,
            waiting: metrics.waiting,
            kv_usage: metrics.kv_usage,
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
