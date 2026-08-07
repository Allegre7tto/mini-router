use crate::epp::candidates::CandidateError;
use crate::types::{RoutingDecision, WorkerKey};
use envoy_grpc_ext_proc::envoy::config::core::v3::{HeaderValue, HeaderValueOption};
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::body_mutation::Mutation;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::{
    BodyMutation, BodyResponse, CommonResponse, HeaderMutation, ImmediateResponse,
    ProcessingResponse,
};
use envoy_grpc_ext_proc::envoy::r#type::v3::{HttpStatus, StatusCode};
use prost_types::value::Kind;

pub const PICK_NAMESPACE: &str = "envoy.lb";
pub const PICK_KEY: &str = "x-gateway-destination-endpoint";
pub const SERVED_KEY: &str = "x-gateway-destination-endpoint-served";

pub fn picked(decision: &RoutingDecision) -> ProcessingResponse {
    let endpoint = decision.endpoint.to_string();

    let header_mutation = HeaderMutation {
        set_headers: vec![HeaderValueOption {
            header: Some(HeaderValue {
                key: PICK_KEY.to_string(),
                value: endpoint.clone(),
                raw_value: vec![],
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    let body_mutation = BodyMutation {
        mutation: Some(Mutation::Body(decision.body.clone())),
    };

    let common_response = CommonResponse {
        status: ResponseStatus::ContinueAndReplace as i32,
        header_mutation: Some(header_mutation),
        body_mutation: Some(body_mutation),
        ..Default::default()
    };

    let mut pick_fields = std::collections::BTreeMap::new();
    pick_fields.insert(
        PICK_KEY.to_string(),
        prost_types::Value {
            kind: Some(Kind::StringValue(endpoint)),
        },
    );
    let mut dynamic_fields = std::collections::BTreeMap::new();
    dynamic_fields.insert(
        PICK_NAMESPACE.to_string(),
        prost_types::Value {
            kind: Some(Kind::StructValue(prost_types::Struct {
                fields: pick_fields,
            })),
        },
    );

    ProcessingResponse {
        response: Some(Response::RequestBody(BodyResponse {
            response: Some(common_response),
        })),
        dynamic_metadata: Some(prost_types::Struct {
            fields: dynamic_fields,
        }),
        ..Default::default()
    }
}

pub fn unavailable(message: &str) -> ProcessingResponse {
    ProcessingResponse {
        response: Some(Response::ImmediateResponse(ImmediateResponse {
            status: Some(HttpStatus {
                code: StatusCode::ServiceUnavailable as i32,
            }),
            body: message.as_bytes().to_vec(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

pub fn served(metadata: &prost_types::Struct) -> Result<Option<WorkerKey>, CandidateError> {
    let lb = match metadata.fields.get(PICK_NAMESPACE) {
        Some(v) => match &v.kind {
            Some(Kind::StructValue(s)) => s,
            _ => return Ok(None),
        },
        None => return Ok(None),
    };

    let endpoint = match lb.fields.get(SERVED_KEY) {
        Some(v) => match &v.kind {
            Some(Kind::StringValue(s)) => s.clone(),
            _ => return Ok(None),
        },
        None => return Ok(None),
    };

    let addr: std::net::SocketAddr = endpoint
        .parse()
        .map_err(|_| CandidateError::InvalidEndpoint(endpoint))?;
    Ok(Some(WorkerKey(addr)))
}
