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
use prost_types::{Struct, Value};
use std::collections::BTreeMap;

pub const PICK_NAMESPACE: &str = "envoy.lb";
pub const PICK_KEY: &str = "x-gateway-destination-endpoint";
pub const SERVED_KEY: &str = "x-gateway-destination-endpoint-served";

pub fn picked(decision: &RoutingDecision) -> ProcessingResponse {
    let endpoint = decision.endpoint.to_string();
    ProcessingResponse {
        response: Some(Response::RequestBody(BodyResponse {
            response: Some(CommonResponse {
                status: ResponseStatus::ContinueAndReplace as i32,
                header_mutation: Some(HeaderMutation {
                    set_headers: vec![HeaderValueOption {
                        header: Some(HeaderValue {
                            key: PICK_KEY.into(),
                            value: endpoint.clone(),
                            raw_value: vec![],
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                body_mutation: Some(BodyMutation {
                    mutation: Some(Mutation::Body(decision.body.clone())),
                }),
                ..Default::default()
            }),
        })),
        dynamic_metadata: Some(Struct {
            fields: BTreeMap::from([(
                PICK_NAMESPACE.into(),
                Value {
                    kind: Some(Kind::StructValue(Struct {
                        fields: BTreeMap::from([(
                            PICK_KEY.into(),
                            Value {
                                kind: Some(Kind::StringValue(endpoint)),
                            },
                        )]),
                    })),
                },
            )]),
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

pub fn served(metadata: &Struct) -> Result<Option<WorkerKey>, CandidateError> {
    let Some(Kind::StructValue(lb)) = metadata
        .fields
        .get(PICK_NAMESPACE)
        .and_then(|v| v.kind.as_ref())
    else {
        return Ok(None);
    };
    let Some(Kind::StringValue(endpoint)) = lb.fields.get(SERVED_KEY).and_then(|v| v.kind.as_ref())
    else {
        return Ok(None);
    };
    let addr = endpoint
        .parse()
        .map_err(|_| CandidateError::InvalidEndpoint(endpoint.clone()))?;
    Ok(Some(WorkerKey(addr)))
}
