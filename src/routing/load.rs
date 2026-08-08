#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("http error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
}

pub struct LoadMetrics {
    pub running: u64,
    pub waiting: u64,
    pub kv_usage: f64,
}

pub fn parse_metrics(text: &str) -> Result<LoadMetrics, LoadError> {
    let mut running = None;
    let mut waiting = None;
    let mut kv_usage = None;
    for line in text.lines() {
        if line.starts_with("vllm:num_requests_running{") {
            running = Some(value(line)?);
        } else if line.starts_with("vllm:num_requests_waiting{") {
            waiting = Some(value(line)?);
        } else if line.starts_with("vllm:kv_cache_usage_perc{") {
            kv_usage = Some(value(line)?);
        }
    }
    Ok(LoadMetrics {
        running: running.unwrap_or(0.0) as u64,
        waiting: waiting.unwrap_or(0.0) as u64,
        kv_usage: kv_usage.unwrap_or(0.0),
    })
}

fn value(line: &str) -> Result<f64, LoadError> {
    let token = line
        .split_whitespace()
        .last()
        .ok_or_else(|| LoadError::Parse("no value".into()))?;
    token
        .parse::<f64>()
        .map_err(|_| LoadError::Parse(format!("invalid value: {token}")))
}
