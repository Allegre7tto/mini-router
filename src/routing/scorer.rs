use crate::config::RouterConfig;
use crate::kv::index::PrefixMatch;
use crate::types::{CandidateSet, WorkerKey};
use crate::workers::endpoint::{EndpointSnapshot, Trust};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
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
    let snap_map: BTreeMap<_, _> = snapshots.iter().map(|s| (&s.key, s)).collect();

    let mut scored: Vec<_> = candidates
        .0
        .iter()
        .filter_map(|w| {
            let snap = snap_map.get(w)?;
            let cache_score = if snap.trust == Trust::Trusted {
                matches
                    .get(w)
                    .map(|m| m.tokens as f64 / token_count as f64)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let effective = (snap
                .load
                .as_ref()
                .map(|l| l.running + l.waiting)
                .unwrap_or(0))
                + snap.reservations as u64;
            let load_score = effective as f64 / config.max_concurrency as f64;
            let score = config.cache_weight * cache_score - config.load_weight * load_score;
            Some(ScoredEndpoint {
                endpoint: w.clone(),
                generation: snap.generation,
                score,
                matched_tokens: matches.get(w).map(|m| m.tokens).unwrap_or(0),
                effective_load: load_score,
            })
        })
        .collect();

    scored.sort_by(|a, b| {
        let cmp = b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal);
        if cmp != Ordering::Equal {
            return cmp;
        }
        let ha = hash_endpoint(request_id, &a.endpoint);
        let hb = hash_endpoint(request_id, &b.endpoint);
        ha.cmp(&hb)
    });

    scored
        .into_iter()
        .next()
        .ok_or(RoutingError::NoEligibleEndpoint)
}

fn hash_endpoint(request_id: &str, endpoint: &WorkerKey) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(request_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(endpoint.to_string().as_bytes());
    hasher.finalize().to_vec()
}
