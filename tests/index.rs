use router::kv::events::{BlockHash, CacheEvent, EventBatch};
use router::kv::index::PrefixIndex;
use router::types::{CandidateSet, WorkerKey};
use std::collections::BTreeMap;

fn worker(s: &str) -> WorkerKey {
    s.parse().unwrap()
}

fn candidates(keys: &[WorkerKey]) -> CandidateSet {
    CandidateSet(keys.iter().cloned().collect())
}

fn store_batch(
    hashes: Vec<BlockHash>,
    parent: Option<BlockHash>,
    tokens: Vec<u32>,
    block_size: usize,
) -> EventBatch {
    EventBatch {
        ts: 1.0,
        events: vec![CacheEvent::Stored {
            hashes,
            parent,
            tokens,
            size: block_size,
            group: 0,
        }],
        data_parallel_rank: None,
    }
}

#[test]
fn match_prefix_two_endpoints() {
    let a = worker("10.0.0.1:8200");
    let b = worker("10.0.0.2:8200");
    let mut index = PrefixIndex::default();

    let a_h1 = BlockHash::Bytes(b"ha1".to_vec());
    let a_h2 = BlockHash::Bytes(b"ha2".to_vec());
    let b_h1 = BlockHash::Bytes(b"hb1".to_vec());
    let b_h2 = BlockHash::Bytes(b"hb2".to_vec());

    index
        .apply(
            a.clone(),
            1,
            &store_batch(
                vec![a_h1.clone(), a_h2.clone()],
                None,
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                4,
            ),
        )
        .unwrap();

    index
        .apply(
            a.clone(),
            1,
            &store_batch(
                vec![a_h1.clone(), a_h2.clone()],
                None,
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                4,
            ),
        )
        .unwrap();

    index
        .apply(
            b.clone(),
            1,
            &store_batch(
                vec![b_h1.clone(), b_h2.clone()],
                None,
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                4,
            ),
        )
        .unwrap();

    let query = vec![1, 2, 3, 4, 5, 6, 7, 8, 99];
    let cands = candidates(&[a.clone(), b.clone()]);
    let gens = BTreeMap::from([(a.clone(), 1), (b.clone(), 1)]);
    let matches = index.match_prefix(&query, &cands, &gens);
    assert_eq!(matches.get(&a).unwrap().tokens, 8);
    assert_eq!(matches.get(&b).unwrap().tokens, 8);

    let cands_a = candidates(std::slice::from_ref(&a));
    let m_a = index.match_prefix(&query, &cands_a, &gens);
    assert_eq!(m_a.get(&a).unwrap().tokens, 8);
    assert!(!m_a.contains_key(&b));
}

#[test]
fn non_candidate_absent() {
    let a = worker("10.0.0.1:8200");
    let b = worker("10.0.0.2:8200");
    let mut index = PrefixIndex::default();
    let h = BlockHash::Bytes(b"h1".to_vec());
    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();
    let cands = candidates(std::slice::from_ref(&b));
    let gens = BTreeMap::from([(a.clone(), 1), (b.clone(), 1)]);
    let matches = index.match_prefix(&[1, 2, 3, 4], &cands, &gens);
    assert!(!matches.contains_key(&a));
}

#[test]
fn remove_clears_ownership() {
    let a = worker("10.0.0.1:8200");
    let mut index = PrefixIndex::default();
    let h = BlockHash::Bytes(b"h1".to_vec());
    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h.clone()], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();

    index
        .apply(
            a.clone(),
            1,
            &EventBatch {
                ts: 1.0,
                events: vec![CacheEvent::Removed {
                    hashes: vec![h],
                    group: 0,
                }],
                data_parallel_rank: None,
            },
        )
        .unwrap();

    let cands = candidates(std::slice::from_ref(&a));
    let gens = BTreeMap::from([(a.clone(), 1)]);
    let matches = index.match_prefix(&[1, 2, 3, 4], &cands, &gens);
    assert_eq!(matches.get(&a).unwrap().tokens, 0);
}

#[test]
fn all_blocks_cleared() {
    let a = worker("10.0.0.1:8200");
    let mut index = PrefixIndex::default();
    let h = BlockHash::Bytes(b"h1".to_vec());
    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();

    index
        .apply(
            a.clone(),
            1,
            &EventBatch {
                ts: 1.0,
                events: vec![CacheEvent::Cleared],
                data_parallel_rank: None,
            },
        )
        .unwrap();

    let cands = candidates(std::slice::from_ref(&a));
    let gens = BTreeMap::from([(a.clone(), 1)]);
    let matches = index.match_prefix(&[1, 2, 3, 4], &cands, &gens);
    assert_eq!(matches.get(&a).unwrap().tokens, 0);
}

#[test]
fn generation_isolation() {
    let a = worker("10.0.0.1:8200");
    let mut index = PrefixIndex::default();
    let h1 = BlockHash::Bytes(b"h1".to_vec());
    let h2 = BlockHash::Bytes(b"h2".to_vec());

    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h1], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();
    index.clear(a.clone(), 1);

    index
        .apply(
            a.clone(),
            2,
            &store_batch(vec![h2], None, vec![5, 6, 7, 8], 4),
        )
        .unwrap();

    let cands = candidates(std::slice::from_ref(&a));
    let gens = BTreeMap::from([(a.clone(), 2)]);
    let matches = index.match_prefix(&[1, 2, 3, 4], &cands, &gens);
    assert_eq!(matches.get(&a).unwrap().tokens, 0);

    let matches2 = index.match_prefix(&[5, 6, 7, 8], &cands, &gens);
    assert_eq!(matches2.get(&a).unwrap().tokens, 4);
}

#[test]
fn delayed_generation1_event_no_effect() {
    let a = worker("10.0.0.1:8200");
    let mut index = PrefixIndex::default();
    let h1 = BlockHash::Bytes(b"h1".to_vec());
    let h2 = BlockHash::Bytes(b"h2".to_vec());

    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h1.clone()], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();
    index.clear(a.clone(), 1);
    index
        .apply(
            a.clone(),
            2,
            &store_batch(vec![h2.clone()], None, vec![5, 6, 7, 8], 4),
        )
        .unwrap();

    index
        .apply(
            a.clone(),
            1,
            &store_batch(vec![h1], None, vec![1, 2, 3, 4], 4),
        )
        .unwrap();

    let cands = candidates(std::slice::from_ref(&a));
    let gens = BTreeMap::from([(a.clone(), 2)]);
    let matches = index.match_prefix(&[1, 2, 3, 4], &cands, &gens);
    assert_eq!(matches.get(&a).unwrap().tokens, 0);
}

#[test]
fn unknown_parent_rejected() {
    let a = worker("10.0.0.1:8200");
    let mut index = PrefixIndex::default();
    let h_child = BlockHash::Bytes(b"h_child".to_vec());
    let unknown = BlockHash::Bytes(b"unknown".to_vec());
    let result = index.apply(
        a.clone(),
        1,
        &store_batch(vec![h_child], Some(unknown), vec![1, 2, 3, 4], 4),
    );
    assert!(result.is_err());
}
