use crate::error::ConfigError;

#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub pool: String,
    pub namespace: String,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub health: String,
    pub model: String,
    pub revision: String,
    pub probe_digest: String,
    pub max_body_bytes: usize,
    pub max_token_count: usize,
    pub max_candidates: usize,
    pub block_size: usize,
    pub renderer_url: String,
    pub renderer_timeout_secs: f64,
    pub cache_weight: f64,
    pub load_weight: f64,
    pub max_concurrency: u64,
    pub metrics_max_age_secs: u64,
    pub metrics_interval_secs: u64,
    pub metrics_port: u16,
    pub endpoint_state_ttl_secs: u64,
    pub reservation_ttl_secs: u64,
    pub kv_event_port: u16,
    pub kv_replay_port: u16,
    pub epp_listen: String,
    pub metrics_listen: String,
    pub workload_token: String,
}

impl RouterConfig {
    pub fn fixture() -> Self {
        RouterConfig {
            pool: "router".into(),
            namespace: "mini-router".into(),
            cert: None,
            key: None,
            health: "0.0.0.0:9002".into(),
            model: "fixed".into(),
            revision: "abc123".into(),
            probe_digest: "sha256:bac02613b6f9456c3b486cbd9e93e575c8a054a7a6784af9e1669950bad5f6e2"
                .into(),
            max_body_bytes: 10_000_000,
            max_token_count: 128_000,
            max_candidates: 32,
            block_size: 16,
            renderer_url: "http://127.0.0.1:8100".into(),
            renderer_timeout_secs: 30.0,
            cache_weight: 1.0,
            load_weight: 1.0,
            max_concurrency: 100,
            metrics_max_age_secs: 10,
            metrics_interval_secs: 5,
            metrics_port: 8000,
            endpoint_state_ttl_secs: 120,
            reservation_ttl_secs: 10,
            kv_event_port: 5557,
            kv_replay_port: 5558,
            epp_listen: "0.0.0.0:9000".into(),
            metrics_listen: "0.0.0.0:9001".into(),
            workload_token: "test-token".into(),
        }
    }

    pub fn env() -> Result<Self, ConfigError> {
        let config = RouterConfig {
            pool: std::env::var("POOL").unwrap_or_else(|_| "router".into()),
            namespace: std::env::var("NAMESPACE")
                .map_err(|_| ConfigError::EmptyField("NAMESPACE".into()))?,
            cert: std::env::var("CERT").ok(),
            key: std::env::var("KEY").ok(),
            health: std::env::var("HEALTH").unwrap_or_else(|_| "0.0.0.0:9002".into()),
            model: std::env::var("MODEL").unwrap_or_else(|_| "fixed".into()),
            revision: std::env::var("REVISION").unwrap_or_else(|_| "abc123".into()),
            probe_digest: std::env::var("PROBE_DIGEST").unwrap_or_else(|_| {
                "sha256:bac02613b6f9456c3b486cbd9e93e575c8a054a7a6784af9e1669950bad5f6e2".into()
            }),
            max_body_bytes: std::env::var("MAX_BODY_BYTES")
                .map_or(Ok(10_000_000), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_BODY_BYTES".into()))?,
            max_token_count: std::env::var("MAX_TOKEN_COUNT")
                .map_or(Ok(128_000), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_TOKEN_COUNT".into()))?,
            max_candidates: std::env::var("MAX_CANDIDATES")
                .map_or(Ok(32), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_CANDIDATES".into()))?,
            block_size: std::env::var("BLOCK_SIZE")
                .map_or(Ok(16), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("BLOCK_SIZE".into()))?,
            renderer_url: std::env::var("RENDERER_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8100".into()),
            renderer_timeout_secs: std::env::var("RENDERER_TIMEOUT_SECS")
                .map_or(Ok(30.0), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("RENDERER_TIMEOUT_SECS".into()))?,
            cache_weight: std::env::var("CACHE_WEIGHT")
                .map_or(Ok(1.0), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("CACHE_WEIGHT".into()))?,
            load_weight: std::env::var("LOAD_WEIGHT")
                .map_or(Ok(1.0), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("LOAD_WEIGHT".into()))?,
            max_concurrency: std::env::var("MAX_CONCURRENCY")
                .map_or(Ok(100), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_CONCURRENCY".into()))?,
            metrics_max_age_secs: std::env::var("METRICS_MAX_AGE_SECS")
                .map_or(Ok(10), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("METRICS_MAX_AGE_SECS".into()))?,
            metrics_interval_secs: std::env::var("METRICS_INTERVAL_SECS")
                .map_or(Ok(5), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("METRICS_INTERVAL_SECS".into()))?,
            metrics_port: std::env::var("METRICS_PORT")
                .map_or(Ok(8000), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("METRICS_PORT".into()))?,
            endpoint_state_ttl_secs: std::env::var("ENDPOINT_STATE_TTL_SECS")
                .map_or(Ok(120), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("ENDPOINT_STATE_TTL_SECS".into()))?,
            reservation_ttl_secs: std::env::var("RESERVATION_TTL_SECS")
                .map_or(Ok(10), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("RESERVATION_TTL_SECS".into()))?,
            kv_event_port: std::env::var("KV_EVENT_PORT")
                .map_or(Ok(5557), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("KV_EVENT_PORT".into()))?,
            kv_replay_port: std::env::var("KV_REPLAY_PORT")
                .map_or(Ok(5558), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("KV_REPLAY_PORT".into()))?,
            epp_listen: std::env::var("EPP_LISTEN").unwrap_or_else(|_| "0.0.0.0:9000".into()),
            metrics_listen: std::env::var("METRICS_LISTEN")
                .unwrap_or_else(|_| "0.0.0.0:9001".into()),
            workload_token: std::env::var("WORKLOAD_TOKEN")
                .map_err(|_| ConfigError::EmptyField("WORKLOAD_TOKEN".into()))?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.cert.is_some() != self.key.is_some()
            || self.cert.as_ref().is_some_and(|value| value.is_empty())
            || self.key.as_ref().is_some_and(|value| value.is_empty())
        {
            return Err(ConfigError::InvalidValue(
                "CERT and KEY must be set together".into(),
            ));
        }
        if self.health.parse::<std::net::SocketAddr>().is_err() {
            return Err(ConfigError::InvalidValue("HEALTH".into()));
        }
        if self.pool.is_empty() {
            return Err(ConfigError::EmptyField("pool".into()));
        }
        if self.namespace.is_empty() {
            return Err(ConfigError::EmptyField("namespace".into()));
        }
        if self.model.is_empty() {
            return Err(ConfigError::EmptyField("model".into()));
        }
        if self.revision.is_empty() {
            return Err(ConfigError::EmptyField("revision".into()));
        }
        if self.probe_digest.is_empty() {
            return Err(ConfigError::EmptyField("probe_digest".into()));
        }
        if self.max_body_bytes == 0 {
            return Err(ConfigError::ZeroValue("max_body_bytes".into()));
        }
        if self.max_token_count == 0 {
            return Err(ConfigError::ZeroValue("max_token_count".into()));
        }
        if self.max_candidates == 0 {
            return Err(ConfigError::ZeroValue("max_candidates".into()));
        }
        if self.block_size == 0 {
            return Err(ConfigError::ZeroValue("block_size".into()));
        }
        if self.cache_weight < 0.0 {
            return Err(ConfigError::NegativeWeight("cache_weight".into()));
        }
        if self.load_weight < 0.0 {
            return Err(ConfigError::NegativeWeight("load_weight".into()));
        }
        if self.max_concurrency == 0 {
            return Err(ConfigError::ZeroValue("max_concurrency".into()));
        }
        if self.metrics_max_age_secs == 0 {
            return Err(ConfigError::ZeroValue("metrics_max_age_secs".into()));
        }
        if self.metrics_interval_secs == 0 {
            return Err(ConfigError::ZeroValue("metrics_interval_secs".into()));
        }
        if self.metrics_port == 0 {
            return Err(ConfigError::ZeroValue("metrics_port".into()));
        }
        if self.endpoint_state_ttl_secs == 0 {
            return Err(ConfigError::ZeroValue("endpoint_state_ttl_secs".into()));
        }
        if self.reservation_ttl_secs == 0 {
            return Err(ConfigError::ZeroValue("reservation_ttl_secs".into()));
        }
        if self.kv_event_port == 0 {
            return Err(ConfigError::ZeroValue("kv_event_port".into()));
        }
        if self.kv_replay_port == 0 {
            return Err(ConfigError::ZeroValue("kv_replay_port".into()));
        }
        if self.workload_token.is_empty() {
            return Err(ConfigError::EmptyField("workload_token".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AdapterConfig {
    pub model: String,
    pub vllm_url: String,
    pub max_body_bytes: usize,
    pub max_token_count: usize,
    pub workload_token: String,
    pub listen: String,
}

impl AdapterConfig {
    pub fn fixture() -> Self {
        AdapterConfig {
            model: "fixed".into(),
            vllm_url: "http://127.0.0.1:8000".into(),
            max_body_bytes: 10_000_000,
            max_token_count: 128_000,
            workload_token: "test-token".into(),
            listen: "0.0.0.0:8200".into(),
        }
    }

    pub fn env() -> Result<Self, ConfigError> {
        let config = AdapterConfig {
            model: std::env::var("MODEL").unwrap_or_else(|_| "fixed".into()),
            vllm_url: std::env::var("VLLM_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".into()),
            max_body_bytes: std::env::var("MAX_BODY_BYTES")
                .map_or(Ok(10_000_000), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_BODY_BYTES".into()))?,
            max_token_count: std::env::var("MAX_TOKEN_COUNT")
                .map_or(Ok(128_000), |v| v.parse())
                .map_err(|_| ConfigError::InvalidValue("MAX_TOKEN_COUNT".into()))?,
            workload_token: std::env::var("WORKLOAD_TOKEN")
                .map_err(|_| ConfigError::EmptyField("WORKLOAD_TOKEN".into()))?,
            listen: std::env::var("ADAPTER_LISTEN").unwrap_or_else(|_| "0.0.0.0:8200".into()),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.model.is_empty() {
            return Err(ConfigError::EmptyField("model".into()));
        }
        if self.vllm_url.is_empty() {
            return Err(ConfigError::EmptyField("vllm_url".into()));
        }
        if self.max_body_bytes == 0 {
            return Err(ConfigError::ZeroValue("max_body_bytes".into()));
        }
        if self.max_token_count == 0 {
            return Err(ConfigError::ZeroValue("max_token_count".into()));
        }
        if self.workload_token.is_empty() {
            return Err(ConfigError::EmptyField("workload_token".into()));
        }
        Ok(())
    }
}
