use crate::kv::events::{BlockHash, CacheEvent, EventBatch};
use crate::types::{CandidateSet, WorkerKey};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub worker: WorkerKey,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct PrefixMatch {
    pub blocks: usize,
    pub tokens: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("unknown parent block")]
    UnknownParent,
}

#[derive(Debug, Clone)]
struct BlockNode {
    parent: Option<BlockHash>,
    tokens: Vec<u32>,
    owners: Vec<(WorkerKey, u64)>,
}

#[derive(Default)]
pub struct PrefixIndex {
    nodes: HashMap<BlockHashMapKey, BlockNode>,
    reverse: HashMap<(WorkerKey, u64), Vec<BlockHashMapKey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockHashMapKey {
    group: u32,
    hash: BlockHash,
}

impl PrefixIndex {
    pub fn apply(
        &mut self,
        worker: WorkerKey,
        generation: u64,
        batch: &EventBatch,
    ) -> Result<(), IndexError> {
        for event in &batch.events {
            match event {
                CacheEvent::Stored {
                    hashes,
                    parent,
                    tokens,
                    size,
                    group,
                } => {
                    if *size == 0 {
                        continue;
                    }
                    let group = *group;
                    for (i, hash) in hashes.iter().enumerate() {
                        let key = BlockHashMapKey {
                            group,
                            hash: hash.clone(),
                        };
                        let block_parent = if i == 0 {
                            parent.clone()
                        } else {
                            hashes.get(i.wrapping_sub(1)).cloned()
                        };
                        let start = i * size;
                        let end = usize::min(start + size, tokens.len());
                        let block_tokens = tokens[start..end].to_vec();

                        if i == 0 && parent.is_some() {
                            let pkey = BlockHashMapKey {
                                group,
                                hash: parent.clone().unwrap(),
                            };
                            if !self.nodes.contains_key(&pkey) {
                                return Err(IndexError::UnknownParent);
                            }
                        } else if i > 0 {
                            let prev = hashes[i - 1].clone();
                            let pkey = BlockHashMapKey { group, hash: prev };
                            if !self.nodes.contains_key(&pkey) {
                                return Err(IndexError::UnknownParent);
                            }
                        }

                        let entry = self.nodes.entry(key.clone()).or_insert_with(|| BlockNode {
                            parent: block_parent.clone(),
                            tokens: block_tokens.clone(),
                            owners: Vec::new(),
                        });
                        entry.tokens = block_tokens;
                        if i == 0 {
                            entry.parent = parent.clone();
                        }
                        if i == 0 {
                            entry.parent = parent.clone();
                        }

                        let owner_key = (worker.clone(), generation);
                        if !entry.owners.contains(&owner_key) {
                            entry.owners.push(owner_key.clone());
                        }
                        self.reverse.entry(owner_key).or_default().push(key);
                    }
                }
                CacheEvent::Removed { hashes, group } => {
                    let group = *group;
                    for hash in hashes {
                        let key = BlockHashMapKey {
                            group,
                            hash: hash.clone(),
                        };
                        if let Some(node) = self.nodes.get_mut(&key) {
                            node.owners
                                .retain(|(w, g)| w != &worker || g != &generation);
                            if node.owners.is_empty() {
                                self.nodes.remove(&key);
                            }
                        }
                        if let Some(v) = self.reverse.get_mut(&(worker.clone(), generation)) {
                            v.retain(|k| k != &key);
                        }
                    }
                }
                CacheEvent::Cleared => {
                    self.clear(worker.clone(), generation);
                }
            }
        }
        Ok(())
    }

    pub fn clear(&mut self, worker: WorkerKey, generation: u64) {
        let owner_key = (worker, generation);
        if let Some(keys) = self.reverse.remove(&owner_key) {
            for key in &keys {
                if let Some(node) = self.nodes.get_mut(key) {
                    node.owners
                        .retain(|(w, g)| w != &owner_key.0 || g != &owner_key.1);
                    if node.owners.is_empty() {
                        self.nodes.remove(key);
                    }
                }
            }
        }
    }

    pub fn match_prefix(
        &self,
        token_ids: &[u32],
        candidates: &CandidateSet,
        generations: &BTreeMap<WorkerKey, u64>,
    ) -> BTreeMap<WorkerKey, PrefixMatch> {
        let mut results = BTreeMap::new();

        for worker in &candidates.0 {
            let epoch = match generations.get(worker) {
                Some(g) => *g,
                None => continue,
            };

            let mut matched_tokens = 0;
            let mut matched_blocks = 0;
            let mut current_hash: Option<BlockHash> = None;

            loop {
                let mut found = false;
                for (key, node) in &self.nodes {
                    if key.group != 0 {
                        continue;
                    }
                    if !node.owners.contains(&(worker.clone(), epoch)) {
                        continue;
                    }
                    if node.parent != current_hash {
                        continue;
                    }
                    let remaining = &token_ids[matched_tokens..];
                    let node_tokens = &node.tokens;
                    let cmp_len = usize::min(remaining.len(), node_tokens.len());
                    if cmp_len == 0 {
                        break;
                    }
                    if node_tokens[..cmp_len] == remaining[..cmp_len] {
                        matched_tokens += cmp_len;
                        matched_blocks += 1;
                        current_hash = Some(key.hash.clone());
                        found = true;
                        break;
                    }
                }
                if !found || matched_tokens >= token_ids.len() {
                    break;
                }
            }

            results.insert(
                worker.clone(),
                PrefixMatch {
                    blocks: matched_blocks,
                    tokens: matched_tokens,
                },
            );
        }

        results
    }
}
