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

pub struct AdapterMetrics {
    registry: Registry,
    pub requests: Counter,
    pub tokenin: Counter,
    pub fallback: Counter,
    pub invalid: Counter,
    pub errors: Counter,
    pub latency: Histogram,
    pub streams: Gauge,
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

impl AdapterMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let requests = Counter::new("adapter_requests_total", "Total adapter requests")?;
        let tokenin = Counter::new("adapter_tokenin_total", "Token-in requests")?;
        let fallback = Counter::new("adapter_fallback_total", "Raw fallback requests")?;
        let invalid = Counter::new("adapter_invalid_total", "Invalid requests")?;
        let errors = Counter::new("adapter_upstream_errors_total", "Upstream errors")?;
        let latency = Histogram::with_opts(HistogramOpts::new(
            "adapter_upstream_latency_seconds",
            "Upstream request latency",
        ))?;
        let streams = Gauge::new("adapter_active_streams", "Active response streams")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(tokenin.clone()))?;
        registry.register(Box::new(fallback.clone()))?;
        registry.register(Box::new(invalid.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(latency.clone()))?;
        registry.register(Box::new(streams.clone()))?;

        Ok(AdapterMetrics {
            registry,
            requests,
            tokenin,
            fallback,
            invalid,
            errors,
            latency,
            streams,
        })
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(e.to_string()))
    }
}
