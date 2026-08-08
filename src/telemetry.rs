use prometheus::{Counter, Encoder, Gauge, Histogram, HistogramOpts, Registry, TextEncoder};

pub struct RouterMetrics {
    registry: Registry,
    pub requests: Counter,
    pub latency: Histogram,
    pub token_count: Histogram,
    pub matched_blocks: Histogram,
    pub matched_tokens: Histogram,
    pub endpoint_decisions: Counter,
    pub fallbacks: Counter,
    pub event_duplicates: Counter,
    pub event_gaps: Counter,
    pub event_replays: Counter,
    pub index_size: Gauge,
    pub reservation_count: Gauge,
    pub active_endpoints: Gauge,
}

impl RouterMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let requests = Counter::new("router_requests_total", "Total routing requests")?;
        let latency = Histogram::with_opts(HistogramOpts::new(
            "router_latency_seconds",
            "Routing latency",
        ))?;
        let token_count = Histogram::with_opts(HistogramOpts::new(
            "router_token_count",
            "Request token count",
        ))?;
        let matched_blocks = Histogram::with_opts(HistogramOpts::new(
            "router_matched_blocks",
            "Matched prefix blocks",
        ))?;
        let matched_tokens = Histogram::with_opts(HistogramOpts::new(
            "router_matched_tokens",
            "Matched prefix tokens",
        ))?;
        let endpoint_decisions = Counter::new(
            "router_endpoint_decisions_total",
            "Selected endpoint counts",
        )?;
        let fallbacks = Counter::new("router_fallbacks_total", "Fallback routing decisions")?;
        let event_duplicates =
            Counter::new("router_event_duplicates_total", "Duplicate KV events")?;
        let event_gaps = Counter::new("router_event_gaps_total", "KV event gaps")?;
        let event_replays = Counter::new("router_event_replays_total", "Replay recoveries")?;
        let index_size = Gauge::new("router_index_size", "Prefix index node count")?;
        let reservation_count = Gauge::new("router_reservation_count", "Active reservations")?;
        let active_endpoints = Gauge::new("router_active_endpoints", "Active endpoint count")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(latency.clone()))?;
        registry.register(Box::new(token_count.clone()))?;
        registry.register(Box::new(matched_blocks.clone()))?;
        registry.register(Box::new(matched_tokens.clone()))?;
        registry.register(Box::new(endpoint_decisions.clone()))?;
        registry.register(Box::new(fallbacks.clone()))?;
        registry.register(Box::new(event_duplicates.clone()))?;
        registry.register(Box::new(event_gaps.clone()))?;
        registry.register(Box::new(event_replays.clone()))?;
        registry.register(Box::new(index_size.clone()))?;
        registry.register(Box::new(reservation_count.clone()))?;
        registry.register(Box::new(active_endpoints.clone()))?;

        Ok(RouterMetrics {
            registry,
            requests,
            latency,
            token_count,
            matched_blocks,
            matched_tokens,
            endpoint_decisions,
            fallbacks,
            event_duplicates,
            event_gaps,
            event_replays,
            index_size,
            reservation_count,
            active_endpoints,
        })
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(e.to_string()))
    }
}
