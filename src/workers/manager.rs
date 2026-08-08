use crate::config::RouterConfig;
use crate::kv::index::PrefixIndex;
use crate::kv::subscriber::{Event, Subscriber};
use crate::routing::load::{LoadError, parse_metrics};
use crate::telemetry::RouterMetrics;
use crate::types::{CandidateSet, WorkerKey};
use crate::workers::endpoint::{EndpointSnapshot, LoadSample, Trust};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TOPIC: &str = "kv-events";

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
    cancel: CancellationToken,
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
    index: Arc<Mutex<PrefixIndex>>,
    sender: mpsc::Sender<(WorkerKey, Event)>,
    client: reqwest::Client,
    config: RouterConfig,
    metrics: Arc<RouterMetrics>,
}

impl EndpointManager {
    pub fn new(
        client: reqwest::Client,
        config: RouterConfig,
        index: Arc<Mutex<PrefixIndex>>,
        sender: mpsc::Sender<(WorkerKey, Event)>,
        metrics: Arc<RouterMetrics>,
    ) -> Self {
        EndpointManager {
            states: Mutex::new(HashMap::new()),
            index,
            sender,
            client,
            config,
            metrics,
        }
    }

    pub async fn observe(&self, candidates: &CandidateSet, now: Instant) {
        let mut states = self.states.lock().await;
        for worker in &candidates.0 {
            states
                .entry(worker.clone())
                .or_insert_with(|| {
                    let cancel = CancellationToken::new();
                    Subscriber::new(worker.clone(), TOPIC.into()).run(
                        self.config.kv_event_port,
                        self.config.kv_replay_port,
                        self.sender.clone(),
                        cancel.clone(),
                    );
                    EndpointState {
                        key: worker.clone(),
                        generation: 1,
                        trust: Trust::Warming,
                        last_load: None,
                        reservations: HashMap::new(),
                        last_seen: now,
                        cancel,
                    }
                })
                .last_seen = now;
        }
        self.metrics.active_endpoints.set(states.len() as f64);
    }

    pub async fn handle(&self, worker: WorkerKey, event: Event) {
        let mut states = self.states.lock().await;
        let Some(state) = states.get_mut(&worker) else {
            return;
        };
        match event {
            Event::Batch(batch) => {
                let g = state.generation;
                let applied = self
                    .index
                    .lock()
                    .await
                    .apply(worker.clone(), g, &batch)
                    .is_ok();
                if applied && state.trust == Trust::Warming {
                    state.trust = Trust::Trusted;
                }
            }
            Event::Gap => {
                self.metrics.event_gaps.inc();
                state.trust = Trust::Untrusted;
            }
            Event::Replayed => {
                self.metrics.event_replays.inc();
                state.trust = Trust::Trusted;
            }
            Event::Reset => {
                let g = state.generation;
                self.index.lock().await.clear(worker.clone(), g);
                state.generation += 1;
                state.trust = Trust::Warming;
            }
            Event::Duplicate => {
                self.metrics.event_duplicates.inc();
            }
        }
        self.metrics
            .index_size
            .set(self.index.lock().await.len() as f64);
    }

    pub async fn snapshots(&self, candidates: &CandidateSet) -> Vec<EndpointSnapshot> {
        let states = self.states.lock().await;
        let now = Instant::now();
        let age = Duration::from_secs(self.config.metrics_max_age_secs);
        candidates
            .0
            .iter()
            .filter_map(|w| {
                states.get(w).map(|s| {
                    let mut snap = s.snapshot();
                    if snap
                        .load
                        .as_ref()
                        .is_some_and(|load| now.duration_since(load.collected_at) >= age)
                    {
                        snap.load = None;
                    }
                    snap
                })
            })
            .collect()
    }

    pub async fn reserve(&self, decision: Uuid, worker: WorkerKey, expires: Instant) {
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(&worker) {
            state.reservations.insert(decision, expires);
        }
        self.metrics.reservation_count.set(
            states
                .values()
                .map(|state| state.reservations.len())
                .sum::<usize>() as f64,
        );
    }

    pub async fn complete(&self, decision: Uuid, served: WorkerKey) -> Completion {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            if state.reservations.remove(&decision).is_some() {
                self.metrics.reservation_count.dec();
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
        let mut stale = Vec::new();
        for state in states.values_mut() {
            state.reservations.retain(|_, expires| *expires > now);
            if now.duration_since(state.last_seen).as_secs() >= self.config.endpoint_state_ttl_secs
            {
                stale.push((state.key.clone(), state.generation));
                state.cancel.cancel();
            }
        }
        for (worker, g) in stale {
            states.remove(&worker);
            self.index.lock().await.clear(worker, g);
        }
        self.metrics.active_endpoints.set(states.len() as f64);
        self.metrics.reservation_count.set(
            states
                .values()
                .map(|state| state.reservations.len())
                .sum::<usize>() as f64,
        );
        self.metrics
            .index_size
            .set(self.index.lock().await.len() as f64);
    }

    pub async fn load(&self, worker: &WorkerKey) -> Result<LoadSample, LoadError> {
        let text = self
            .client
            .get(format!(
                "http://{}/metrics",
                worker.metrics(self.config.metrics_port)
            ))
            .send()
            .await
            .map_err(|e| LoadError::Http(e.to_string()))?
            .error_for_status()
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

    pub async fn refresh(&self) {
        let workers: Vec<_> = self.states.lock().await.keys().cloned().collect();
        for worker in workers {
            if let Ok(sample) = self.load(&worker).await {
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(&worker) {
                    state.last_load = Some(sample);
                }
            }
        }
    }
}
