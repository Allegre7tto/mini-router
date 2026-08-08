use crate::kv::events::{BlockHash, CacheEvent, EventBatch};
use crate::types::{CandidateSet, WorkerKey};
use std::collections::{BTreeMap, HashMap};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    group: u32,
    hash: BlockHash,
}

#[derive(Debug, Clone)]
struct Node {
    parent: Option<BlockHash>,
    tokens: Vec<u32>,
    owners: Vec<(WorkerKey, u64)>,
}

#[derive(Default)]
pub struct PrefixIndex {
    nodes: HashMap<Key, Node>,
    reverse: HashMap<(WorkerKey, u64), Vec<Key>>,
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
                    for (i, hash) in hashes.iter().enumerate() {
                        let key = Key {
                            group: *group,
                            hash: hash.clone(),
                        };
                        let parent = if i == 0 {
                            parent.clone()
                        } else {
                            Some(hashes[i - 1].clone())
                        };
                        if let Some(p) = &parent
                            && !self.nodes.contains_key(&Key {
                                group: *group,
                                hash: p.clone(),
                            })
                        {
                            return Err(IndexError::UnknownParent);
                        }

                        let start = i * size;
                        let end = usize::min(start + size, tokens.len());
                        let block = tokens[start..end].to_vec();
                        let entry = self.nodes.entry(key.clone()).or_insert_with(|| Node {
                            parent: parent.clone(),
                            tokens: block.clone(),
                            owners: Vec::new(),
                        });
                        entry.tokens = block;
                        if i == 0 {
                            entry.parent = parent.clone();
                        }

                        let owner = (worker.clone(), generation);
                        if !entry.owners.contains(&owner) {
                            entry.owners.push(owner.clone());
                            self.reverse.entry(owner).or_default().push(key);
                        }
                    }
                }
                CacheEvent::Removed { hashes, group } => {
                    for hash in hashes {
                        let key = Key {
                            group: *group,
                            hash: hash.clone(),
                        };
                        if let Some(node) = self.nodes.get_mut(&key) {
                            node.owners
                                .retain(|(w, g)| w != &worker || g != &generation);
                            if node.owners.is_empty() {
                                self.nodes.remove(&key);
                            }
                        }
                        if let Some(keys) = self.reverse.get_mut(&(worker.clone(), generation)) {
                            keys.retain(|k| k != &key);
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
        let owner = (worker, generation);
        if let Some(keys) = self.reverse.remove(&owner) {
            for key in &keys {
                if let Some(node) = self.nodes.get_mut(key) {
                    node.owners.retain(|(w, g)| w != &owner.0 || g != &owner.1);
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
            let Some(&epoch) = generations.get(worker) else {
                continue;
            };
            let mut tokens = 0;
            let mut blocks = 0;
            let mut parent: Option<BlockHash> = None;
            loop {
                let mut found = false;
                for (key, node) in &self.nodes {
                    if key.group != 0
                        || node.parent != parent
                        || !node.owners.contains(&(worker.clone(), epoch))
                    {
                        continue;
                    }
                    let rest = &token_ids[tokens..];
                    let n = usize::min(rest.len(), node.tokens.len());
                    if n == 0 {
                        break;
                    }
                    if node.tokens[..n] == rest[..n] {
                        tokens += n;
                        blocks += 1;
                        parent = Some(key.hash.clone());
                        found = true;
                        break;
                    }
                }
                if !found || tokens >= token_ids.len() {
                    break;
                }
            }
            results.insert(worker.clone(), PrefixMatch { blocks, tokens });
        }
        results
    }
}
