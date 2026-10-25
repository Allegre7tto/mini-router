use crate::epp::candidates::CandidateError;
use crate::types::{CandidateSet, WorkerKey};
use futures_util::TryStreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client, Resource, ResourceExt};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Pool {
    selector: Selector,
    target_ports: Vec<Port>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Selector {
    match_labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct Port {
    number: u16,
}

#[derive(Default)]
struct State {
    pool: Option<Pool>,
    pods: HashMap<String, Pod>,
    pools_ready: bool,
    pods_ready: bool,
}

/// A read-only view of one namespaced InferencePool and its ready Pod endpoints.
#[derive(Default)]
pub struct Discovery {
    state: RwLock<State>,
}

impl Discovery {
    pub fn ready(&self) -> bool {
        let state = self.state.read().unwrap();
        state.pools_ready && state.pods_ready && state.pool.is_some()
    }

    pub fn candidates(
        &self,
        subset: Option<&CandidateSet>,
        limit: usize,
    ) -> Result<CandidateSet, CandidateError> {
        let state = self.state.read().unwrap();
        if !state.pools_ready || !state.pods_ready {
            return Err(CandidateError::DiscoveryUnavailable);
        }
        let pool = state
            .pool
            .as_ref()
            .ok_or(CandidateError::DiscoveryUnavailable)?;
        let mut candidates = CandidateSet::default();
        for pod in state.pods.values() {
            if pod.metadata.deletion_timestamp.is_some()
                || !pool.selector.match_labels.iter().all(|(key, value)| {
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get(key))
                        == Some(value)
                })
            {
                continue;
            }
            let Some(status) = &pod.status else { continue };
            if status.phase.as_deref() != Some("Running")
                || !status.conditions.as_ref().is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
            {
                continue;
            }
            // Use the primary Pod IP, matching Kubernetes service endpoint selection.
            let Some(ip) = status
                .pod_ip
                .as_ref()
                .and_then(|ip| ip.parse::<IpAddr>().ok())
            else {
                continue;
            };
            for port in &pool.target_ports {
                let worker = WorkerKey(SocketAddr::new(ip, port.number));
                if subset.is_none_or(|subset| subset.0.contains(&worker)) {
                    candidates.0.insert(worker);
                    if candidates.len() > limit {
                        return Err(CandidateError::TooManyCandidates);
                    }
                }
            }
        }
        if candidates.is_empty() {
            return Err(CandidateError::EmptySubset);
        }
        Ok(candidates)
    }

    pub fn pool(&self, event: Event<DynamicObject>) {
        let mut state = self.state.write().unwrap();
        match event {
            Event::Init => {
                state.pools_ready = false;
                state.pool = None;
            }
            Event::InitDone => state.pools_ready = true,
            Event::Delete(_) => state.pool = None,
            Event::Apply(object) | Event::InitApply(object) => {
                state.pool = object.data.get("spec").and_then(|spec| {
                    serde_json::from_value::<Pool>(spec.clone())
                        .inspect_err(|error| tracing::warn!(%error, "invalid InferencePool"))
                        .ok()
                        .filter(|pool| {
                            !pool.selector.match_labels.is_empty()
                                && !pool.target_ports.is_empty()
                                && pool.target_ports.iter().all(|port| port.number != 0)
                        })
                });
            }
        }
    }

    pub fn pod(&self, event: Event<Pod>) {
        let mut state = self.state.write().unwrap();
        match event {
            Event::Init => {
                state.pods_ready = false;
                state.pods.clear();
            }
            Event::InitDone => state.pods_ready = true,
            Event::Delete(pod) => {
                state.pods.remove(&pod.name_any());
            }
            Event::Apply(pod) | Event::InitApply(pod) => {
                state.pods.insert(pod.name_any(), pod);
            }
        }
    }

    pub async fn run(&self, client: Client, namespace: &str, name: &str) {
        let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(
            "inference.networking.k8s.io",
            "v1",
            "InferencePool",
        ));
        let pools = Api::<DynamicObject>::namespaced_with(client.clone(), namespace, &resource);
        let pods = Api::<Pod>::namespaced(client, namespace);
        tokio::join!(
            watch(
                pools,
                watcher::Config::default().fields(&format!("metadata.name={name}")),
                |event| self.pool(event)
            ),
            watch(pods, watcher::Config::default(), |event| self.pod(event)),
        );
    }
}

async fn watch<K>(api: Api<K>, config: watcher::Config, update: impl Fn(Event<K>))
where
    K: Resource + Clone + Debug + serde::de::DeserializeOwned + Send + 'static,
{
    loop {
        let stream = watcher::watcher(api.clone(), config.clone());
        futures_util::pin_mut!(stream);
        loop {
            match stream.try_next().await {
                Ok(Some(event)) => update(event),
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "membership watch failed; resynchronizing");
                    break;
                }
            }
        }
        // Never route from a cache after a watch error. Re-list before serving again.
        update(Event::Init);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
