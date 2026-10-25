#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("empty field: {0}")]
    EmptyField(String),
    #[error("zero value not allowed: {0}")]
    ZeroValue(String),
    #[error("negative weight: {0}")]
    NegativeWeight(String),
    #[error("invalid value: {0}")]
    InvalidValue(String),
}
