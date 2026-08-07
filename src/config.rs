use crate::error::ConfigError;

#[derive(Debug, Clone)]
pub struct RouterConfig {
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
    pub endpoint_state_ttl_secs: u64,
    pub reservation_ttl_secs: u64,
    pub epp_listen: String,
    pub metrics_listen: String,
}

impl RouterConfig {
    pub fn fixture() -> Self {
        RouterConfig {
            model: "fixed".into(),
            revision: "abc123".into(),
            probe_digest: "sha256:abc".into(),
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
            endpoint_state_ttl_secs: 120,
            reservation_ttl_secs: 10,
            epp_listen: "0.0.0.0:9000".into(),
            metrics_listen: "0.0.0.0:9001".into(),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
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
        if self.endpoint_state_ttl_secs == 0 {
            return Err(ConfigError::ZeroValue("endpoint_state_ttl_secs".into()));
        }
        if self.reservation_ttl_secs == 0 {
            return Err(ConfigError::ZeroValue("reservation_ttl_secs".into()));
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
