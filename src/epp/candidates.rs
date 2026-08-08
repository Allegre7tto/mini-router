use crate::types::{CandidateSet, WorkerKey};
use prost_types::value::Kind;
use std::collections::BTreeSet;
use std::net::SocketAddr;

pub const SUBSET_NAMESPACE: &str = "envoy.lb.subset_hint";
pub const SUBSET_KEY: &str = "x-gateway-destination-endpoint-subset";

#[derive(Debug, thiserror::Error)]
pub enum CandidateError {
    #[error("missing envoy.lb.subset_hint in metadata")]
    MissingSubset,
    #[error("empty or missing x-gateway-destination-endpoint-subset")]
    EmptySubset,
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("too many candidates")]
    TooManyCandidates,
    #[error("port zero not allowed: {0}")]
    PortZero(String),
}

pub fn parse(metadata: &prost_types::Struct, limit: usize) -> Result<CandidateSet, CandidateError> {
    let hint = metadata
        .fields
        .get(SUBSET_NAMESPACE)
        .ok_or(CandidateError::MissingSubset)?;
    let Some(Kind::StructValue(subset)) = &hint.kind else {
        return Err(CandidateError::MissingSubset);
    };
    let Some(Kind::ListValue(list)) = subset.fields.get(SUBSET_KEY).and_then(|v| v.kind.as_ref())
    else {
        return Err(CandidateError::EmptySubset);
    };

    if list.values.is_empty() {
        return Err(CandidateError::EmptySubset);
    }
    if list.values.len() > limit {
        return Err(CandidateError::TooManyCandidates);
    }

    let mut seen = BTreeSet::new();
    for v in &list.values {
        let Some(Kind::StringValue(s)) = &v.kind else {
            return Err(CandidateError::InvalidEndpoint("non-string value".into()));
        };
        let addr: SocketAddr = s
            .parse()
            .map_err(|_| CandidateError::InvalidEndpoint(s.clone()))?;
        if addr.port() == 0 {
            return Err(CandidateError::PortZero(s.clone()));
        }
        seen.insert(WorkerKey(addr));
    }
    Ok(CandidateSet(seen))
}
