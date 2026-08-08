use crate::config::RouterConfig;
use crate::kv::index::PrefixMatch;
use crate::types::{CandidateSet, WorkerKey};
use crate::workers::endpoint::{EndpointSnapshot, Trust};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("no eligible endpoint")]
    NoEligibleEndpoint,
}

#[derive(Debug, Clone)]
pub struct ScoredEndpoint {
    pub endpoint: WorkerKey,
    pub generation: u64,
    pub score: f64,
    pub matched_tokens: usize,
    pub effective_load: f64,
}

pub fn choose(
    request_id: &str,
    token_count: usize,
    candidates: &CandidateSet,
    matches: &BTreeMap<WorkerKey, PrefixMatch>,
    snapshots: &[EndpointSnapshot],
    config: &RouterConfig,
) -> Result<ScoredEndpoint, RoutingError> {
    let snaps: BTreeMap<_, _> = snapshots.iter().map(|s| (&s.key, s)).collect();
    let worst = snapshots
        .iter()
        .filter_map(|s| s.load.as_ref().map(|l| l.running + l.waiting))
        .max();

    candidates
        .0
        .iter()
        .filter_map(|w| {
            let snap = snaps.get(w)?;
            let cache = if snap.trust == Trust::Trusted {
                matches
                    .get(w)
                    .map(|m| m.tokens as f64 / token_count as f64)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let load = (snap
                .load
                .as_ref()
                .map(|l| l.running + l.waiting)
                .unwrap_or_else(|| worst.map_or(0, |load| load + 1))
                + snap.reservations as u64) as f64
                / config.max_concurrency as f64;
            Some(ScoredEndpoint {
                endpoint: w.clone(),
                generation: snap.generation,
                score: config.cache_weight * cache - config.load_weight * load,
                matched_tokens: matches.get(w).map(|m| m.tokens).unwrap_or(0),
                effective_load: load,
            })
        })
        .max_by(|a, b| {
            a.score.total_cmp(&b.score).then_with(|| {
                tiebreak(request_id, &b.endpoint).cmp(&tiebreak(request_id, &a.endpoint))
            })
        })
        .ok_or(RoutingError::NoEligibleEndpoint)
}

fn tiebreak(request_id: &str, endpoint: &WorkerKey) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(request_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(endpoint.to_string().as_bytes());
    hasher.finalize().into()
}
