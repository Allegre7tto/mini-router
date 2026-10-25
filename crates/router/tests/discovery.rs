use k8s_openapi::api::core::v1::Pod;
use kube::api::DynamicObject;
use kube::runtime::watcher::Event;
use router::epp::candidates::CandidateError;
use router::types::CandidateSet;
use router::workers::discovery::Discovery;
use serde_json::json;

fn pool(label: &str, ports: &[u16]) -> DynamicObject {
    serde_json::from_value(json!({
        "apiVersion": "inference.networking.k8s.io/v1", "kind": "InferencePool",
        "metadata": {"name": "router"},
        "spec": {
            "selector": {"matchLabels": {"app": label}},
            "targetPorts": ports.iter().map(|port| json!({"number": port})).collect::<Vec<_>>()
        }
    }))
    .unwrap()
}

fn pod(name: &str, label: &str, ip: &str, ready: bool) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": name, "labels": {"app": label}},
        "status": {"phase": "Running", "podIP": ip,
            "conditions": [{"type": "Ready", "status": if ready {"True"} else {"False"}}]}
    }))
    .unwrap()
}

fn discovered() -> Discovery {
    let discovery = Discovery::default();
    discovery.pool(Event::Init);
    discovery.pool(Event::InitApply(pool("worker", &[8200])));
    discovery.pool(Event::InitDone);
    discovery.pod(Event::Init);
    discovery.pod(Event::InitApply(pod("one", "worker", "10.0.0.1", true)));
    discovery.pod(Event::InitDone);
    discovery
}

#[test]
fn requires_both_initial_lists_and_a_valid_pool() {
    let discovery = Discovery::default();
    assert!(!discovery.ready());
    assert!(matches!(
        discovery.candidates(None, 8),
        Err(CandidateError::DiscoveryUnavailable)
    ));
    discovery.pool(Event::InitApply(pool("worker", &[8200])));
    discovery.pool(Event::InitDone);
    assert!(!discovery.ready());
    discovery.pod(Event::InitDone);
    assert!(discovery.ready());
    assert!(discovery.candidates(None, 8).is_err());
    discovery.pool(Event::Apply(pool("worker", &[0])));
    assert!(!discovery.ready());
}

#[test]
fn filters_labels_readiness_termination_phase_and_addresses() {
    let discovery = discovered();
    discovery.pod(Event::Apply(pod("other", "router", "10.0.0.2", true)));
    discovery.pod(Event::Apply(pod("unready", "worker", "10.0.0.3", false)));
    discovery.pod(Event::Apply(pod("invalid", "worker", "not-an-ip", true)));
    let mut terminating = pod("terminating", "worker", "10.0.0.4", true);
    terminating.metadata.deletion_timestamp =
        Some(serde_json::from_value(json!("2026-10-25T00:00:00Z")).unwrap());
    discovery.pod(Event::Apply(terminating));
    let mut pending = pod("pending", "worker", "10.0.0.5", true);
    pending.status.as_mut().unwrap().phase = Some("Pending".into());
    discovery.pod(Event::Apply(pending));
    let candidates = discovery.candidates(None, 8).unwrap();
    assert_eq!(
        candidates,
        CandidateSet(["10.0.0.1:8200".parse().unwrap()].into())
    );
}

#[test]
fn subset_can_only_restrict_discovered_members() {
    let discovery = discovered();
    discovery.pod(Event::Apply(pod("two", "worker", "10.0.0.2", true)));
    let subset = CandidateSet(
        [
            "10.0.0.1:8200".parse().unwrap(),
            "10.0.0.99:8200".parse().unwrap(),
        ]
        .into(),
    );
    assert_eq!(discovery.candidates(Some(&subset), 8).unwrap().len(), 1);
    let outsider = CandidateSet(["10.0.0.99:8200".parse().unwrap()].into());
    assert!(discovery.candidates(Some(&outsider), 8).is_err());
    assert!(
        discovery
            .candidates(Some(&CandidateSet::default()), 8)
            .is_err()
    );
    assert!(matches!(
        discovery.candidates(None, 1),
        Err(CandidateError::TooManyCandidates)
    ));
    assert_eq!(discovery.candidates(Some(&subset), 1).unwrap().len(), 1);
}

#[test]
fn updates_deletions_and_relists_replace_membership() {
    let discovery = discovered();
    discovery.pod(Event::Apply(pod("one", "worker", "10.0.0.1", false)));
    assert!(discovery.candidates(None, 8).is_err());
    discovery.pod(Event::Apply(pod("one", "worker", "10.0.0.1", true)));
    discovery.pool(Event::Apply(pool("worker", &[8300, 8400])));
    let candidates = discovery.candidates(None, 8).unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.0.contains(&"10.0.0.1:8300".parse().unwrap()));
    discovery.pool(Event::Apply(pool("new", &[8200])));
    assert!(discovery.candidates(None, 8).is_err());
    discovery.pool(Event::Apply(pool("worker", &[8200])));
    discovery.pod(Event::Delete(pod("one", "worker", "10.0.0.1", true)));
    assert!(discovery.candidates(None, 8).is_err());
    discovery.pod(Event::Init);
    assert!(!discovery.ready());
    discovery.pod(Event::InitApply(pod("new", "worker", "2001:db8::1", true)));
    assert!(discovery.candidates(None, 8).is_err());
    discovery.pod(Event::InitDone);
    assert_eq!(
        discovery
            .candidates(None, 8)
            .unwrap()
            .0
            .first()
            .unwrap()
            .to_string(),
        "[2001:db8::1]:8200"
    );
    discovery.pool(Event::Delete(pool("worker", &[8200])));
    assert!(!discovery.ready());
    assert!(discovery.candidates(None, 8).is_err());
}

#[tokio::test]
async fn watch_failure_invalidates_cache_until_a_successful_relist() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

    let api = MockServer::start().await;
    let lists = Arc::new(AtomicUsize::new(0));
    let attempts = lists.clone();
    Mock::given(matchers::path("/apis/inference.networking.k8s.io/v1/namespaces/test/inferencepools"))
        .and(matchers::query_param("fieldSelector", "metadata.name=router"))
        .respond_with(move |_: &wiremock::Request| {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200).set_body_json(json!({
                    "apiVersion": "inference.networking.k8s.io/v1", "kind": "InferencePoolList",
                    "metadata": {"resourceVersion": "1"}, "items": [pool("worker", &[8200])]
                }))
            } else {
                ResponseTemplate::new(503).set_body_json(json!({
                    "apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "ServiceUnavailable", "message": "unavailable", "code": 503
                }))
            }
        }).mount(&api).await;
    Mock::given(matchers::path("/api/v1/namespaces/test/pods"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "1"},
            "items": [pod("one", "worker", "10.0.0.1", true)]
        })))
        .mount(&api)
        .await;
    Mock::given(matchers::query_param("watch", "true"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Forbidden", "message": "watch denied", "code": 403
        })).set_delay(Duration::from_millis(200)))
        .with_priority(1).mount(&api).await;
    let client = kube::Client::try_from(kube::Config::new(api.uri().parse().unwrap())).unwrap();
    let discovery = Arc::new(Discovery::default());
    let task = tokio::spawn({
        let discovery = discovery.clone();
        async move { discovery.run(client, "test", "router").await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !discovery.ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(discovery.candidates(None, 8).unwrap().len(), 1);
        while discovery.ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(matches!(
            discovery.candidates(None, 8),
            Err(CandidateError::DiscoveryUnavailable)
        ));
        while lists.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!discovery.ready());
    })
    .await
    .unwrap();
    task.abort();
}
