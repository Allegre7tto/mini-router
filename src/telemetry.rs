use prometheus::{Counter, Encoder, Gauge, Histogram, HistogramOpts, Registry, TextEncoder};

pub struct RouterMetrics {
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
        Ok(RouterMetrics {
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

    pub fn registry(&self) -> Registry {
        let r = Registry::new();
        r.register(Box::new(self.requests.clone())).ok();
        r.register(Box::new(self.latency.clone())).ok();
        r.register(Box::new(self.token_count.clone())).ok();
        r.register(Box::new(self.matched_blocks.clone())).ok();
        r.register(Box::new(self.matched_tokens.clone())).ok();
        r.register(Box::new(self.endpoint_decisions.clone())).ok();
        r.register(Box::new(self.fallbacks.clone())).ok();
        r.register(Box::new(self.event_duplicates.clone())).ok();
        r.register(Box::new(self.event_gaps.clone())).ok();
        r.register(Box::new(self.event_replays.clone())).ok();
        r.register(Box::new(self.index_size.clone())).ok();
        r.register(Box::new(self.reservation_count.clone())).ok();
        r.register(Box::new(self.active_endpoints.clone())).ok();
        r
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder.encode(&self.registry().gather(), &mut buffer)?;
        String::from_utf8(buffer).map_err(|e| prometheus::Error::Msg(e.to_string()))
    }
}
