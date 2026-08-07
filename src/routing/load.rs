#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("http error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
}

pub fn parse_metrics(text: &str) -> Result<LoadMetrics, LoadError> {
    let lines: Vec<&str> = text.lines().collect();
    let mut running = None;
    let mut waiting = None;
    let mut kv_usage = None;

    for line in &lines {
        if line.starts_with("vllm:num_requests_running{") {
            running = Some(parse_value(line)?);
        } else if line.starts_with("vllm:num_requests_waiting{") {
            waiting = Some(parse_value(line)?);
        } else if line.starts_with("vllm:kv_cache_usage_perc{") {
            kv_usage = Some(parse_float(line)?);
        }
    }

    Ok(LoadMetrics {
        running: running.unwrap_or(0),
        waiting: waiting.unwrap_or(0),
        kv_usage: kv_usage.unwrap_or(0.0),
    })
}

pub struct LoadMetrics {
    pub running: u64,
    pub waiting: u64,
    pub kv_usage: f64,
}

fn parse_value(line: &str) -> Result<u64, LoadError> {
    let val_str = line
        .split_whitespace()
        .last()
        .ok_or_else(|| LoadError::Parse("no value".into()))?;
    val_str
        .parse::<f64>()
        .map(|v| v as u64)
        .or_else(|_| val_str.parse::<u64>())
        .map_err(|_| LoadError::Parse(format!("invalid value: {val_str}")))
}

fn parse_float(line: &str) -> Result<f64, LoadError> {
    let val_str = line
        .split_whitespace()
        .last()
        .ok_or_else(|| LoadError::Parse("no value".into()))?;
    val_str
        .parse::<f64>()
        .map_err(|_| LoadError::Parse(format!("invalid float: {val_str}")))
}
