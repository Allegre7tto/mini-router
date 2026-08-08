use router::telemetry::{AdapterMetrics, RouterMetrics};

#[test]
fn router_metrics_encode() {
    let metrics = RouterMetrics::new().unwrap();
    metrics.requests.inc();
    let text = metrics.encode().unwrap();
    assert!(text.contains("router_requests_total 1"));
    assert!(text.contains("router_latency_seconds"));
    assert!(text.contains("router_active_endpoints"));
}

#[test]
fn adapter_metrics_encode() {
    let metrics = AdapterMetrics::new().unwrap();
    metrics.requests.inc();
    let text = metrics.encode().unwrap();
    assert!(text.contains("adapter_requests_total 1"));
    assert!(text.contains("adapter_upstream_latency_seconds"));
    assert!(text.contains("adapter_active_streams"));
}
