use router::config::RouterConfig;
use router::routing::scorer::choose;
use router::types::{CandidateSet, WorkerKey};
use router::workers::endpoint::{EndpointSnapshot, LoadSample, Trust};
use std::collections::BTreeMap;
use std::time::Instant;

fn snap(key: &str, running: u64, waiting: u64, epoch: u64, trust: Trust) -> EndpointSnapshot {
    EndpointSnapshot {
        key: key.parse().unwrap(),
        generation: epoch,
        trust,
        load: Some(LoadSample {
            running,
            waiting,
            kv_usage: 0.0,
            collected_at: Instant::now(),
        }),
        reservations: 0,
    }
}

fn cands(keys: &[&str]) -> CandidateSet {
    CandidateSet(keys.iter().map(|k| k.parse().unwrap()).collect())
}

#[test]
fn cache_rich_wins() {
    let config = RouterConfig::fixture();
    let a = snap("10.0.0.1:8200", 1, 1, 1, Trust::Trusted);
    let b = snap("10.0.0.2:8200", 1, 1, 1, Trust::Trusted);
    let candidates = cands(&["10.0.0.1:8200", "10.0.0.2:8200"]);
    let mut matches = BTreeMap::new();
    matches.insert(
        "10.0.0.1:8200".parse::<WorkerKey>().unwrap(),
        router::kv::index::PrefixMatch {
            blocks: 2,
            tokens: 10,
        },
    );
    matches.insert(
        "10.0.0.2:8200".parse::<WorkerKey>().unwrap(),
        router::kv::index::PrefixMatch {
            blocks: 1,
            tokens: 5,
        },
    );
    let result = choose("req-1", 10, &candidates, &matches, &[a, b], &config).unwrap();
    assert_eq!(result.endpoint.to_string(), "10.0.0.1:8200");
}

#[test]
fn overloaded_worker_loses() {
    let config = RouterConfig::fixture();
    let a = snap("10.0.0.1:8200", 99, 10, 1, Trust::Trusted);
    let b = snap("10.0.0.2:8200", 1, 1, 1, Trust::Trusted);
    let candidates = cands(&["10.0.0.1:8200", "10.0.0.2:8200"]);
    let matches = BTreeMap::new();
    let result = choose("req-1", 10, &candidates, &matches, &[a, b], &config).unwrap();
    assert_eq!(result.endpoint.to_string(), "10.0.0.2:8200");
}

#[test]
fn untrusted_no_cache_bonus() {
    let config = RouterConfig::fixture();
    let a = snap("10.0.0.1:8200", 1, 1, 1, Trust::Untrusted);
    let b = snap("10.0.0.2:8200", 1, 1, 1, Trust::Trusted);
    let candidates = cands(&["10.0.0.1:8200", "10.0.0.2:8200"]);
    let mut matches = BTreeMap::new();
    matches.insert(
        "10.0.0.1:8200".parse::<WorkerKey>().unwrap(),
        router::kv::index::PrefixMatch {
            blocks: 2,
            tokens: 10,
        },
    );
    let result = choose("req-1", 10, &candidates, &matches, &[a, b], &config).unwrap();
    assert_eq!(result.endpoint.to_string(), "10.0.0.2:8200");
}

#[test]
fn deterministic_tie() {
    let config = RouterConfig::fixture();
    let a = snap("10.0.0.1:8200", 1, 1, 1, Trust::Trusted);
    let b = snap("10.0.0.2:8200", 1, 1, 1, Trust::Trusted);
    let candidates = cands(&["10.0.0.1:8200", "10.0.0.2:8200"]);
    let matches = BTreeMap::new();
    let a_snap = a.clone();
    let b_snap = b.clone();
    let result = choose("req-a", 10, &candidates, &matches, &[a, b], &config).unwrap();
    let chosen_a = result.endpoint.to_string();
    for _ in 0..100 {
        let r = choose(
            "req-a",
            10,
            &candidates,
            &matches,
            &[a_snap.clone(), b_snap.clone()],
            &config,
        )
        .unwrap();
        assert_eq!(r.endpoint.to_string(), chosen_a);
    }
}

#[test]
fn candidate_boundary_respected() {
    let config = RouterConfig::fixture();
    let a = snap("10.0.0.1:8200", 1, 1, 1, Trust::Trusted);
    let candidates = cands(&["10.0.0.1:8200"]);
    let matches = BTreeMap::new();
    let result = choose("req-1", 10, &candidates, &matches, &[a], &config).unwrap();
    assert_eq!(result.endpoint.to_string(), "10.0.0.1:8200");
}
