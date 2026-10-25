use criterion::{Criterion, criterion_group, criterion_main};
use router::kv::events::{BlockHash, CacheEvent, EventBatch};
use router::kv::index::PrefixIndex;
use router::types::WorkerKey;
use std::hint::black_box;

fn index_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix-index");
    let worker: WorkerKey = "10.0.0.1:8200".parse().unwrap();

    group.bench_function("apply_store", |b| {
        let mut index = PrefixIndex::default();
        let batch = EventBatch {
            ts: 1.0,
            events: vec![CacheEvent::Stored {
                hashes: vec![BlockHash::Bytes(b"h1".to_vec())],
                parent: None,
                tokens: vec![1, 2, 3, 4],
                size: 4,
                group: 0,
            }],
            data_parallel_rank: None,
        };
        b.iter(|| {
            index
                .apply(black_box(worker.clone()), 1, black_box(&batch))
                .unwrap();
        });
    });

    group.bench_function("match_prefix_16_candidates", |b| {
        let mut index = PrefixIndex::default();
        let hash = BlockHash::Bytes(b"h1".to_vec());
        let batch = EventBatch {
            ts: 1.0,
            events: vec![CacheEvent::Stored {
                hashes: vec![hash],
                parent: None,
                tokens: vec![1, 2, 3, 4],
                size: 4,
                group: 0,
            }],
            data_parallel_rank: None,
        };
        for i in 0..16 {
            let w: WorkerKey = format!("10.0.0.{}:8200", i + 1).parse().unwrap();
            index.apply(w.clone(), 1, &batch).unwrap();
        }
        let candidates: Vec<_> = (1..=16)
            .map(|i| -> WorkerKey { format!("10.0.0.{i}:8200").parse().unwrap() })
            .collect();
        let set = router::types::CandidateSet(candidates.iter().cloned().collect());
        let gens: std::collections::BTreeMap<_, _> =
            candidates.iter().map(|w| (w.clone(), 1)).collect();
        let tokens = vec![1, 2, 3, 4];
        b.iter(|| {
            index.match_prefix(&tokens, &set, &gens);
        });
    });

    group.finish();
}

criterion_group!(benches, index_bench);
criterion_main!(benches);
