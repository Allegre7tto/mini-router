use crate::epp::candidates::CandidateError;
use crate::types::{RoutingDecision, WorkerKey};
use extproc::envoy::config::core::v3::header_value_option::HeaderAppendAction;
use extproc::envoy::config::core::v3::{HeaderValue, HeaderValueOption};
use extproc::envoy::service::ext_proc::v3::body_mutation::Mutation;
use extproc::envoy::service::ext_proc::v3::processing_response::Response;
use extproc::envoy::service::ext_proc::v3::{
    BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, ImmediateResponse,
    ProcessingResponse, StreamedBodyResponse, TrailersResponse,
};
use extproc::envoy::r#type::v3::{HttpStatus, StatusCode};
use prost_types::value::Kind;
use prost_types::{Struct, Value};
use std::collections::{BTreeMap, HashMap};

pub const PICK_NAMESPACE: &str = "envoy.lb";
pub const PICK_KEY: &str = "x-gateway-destination-endpoint";
pub const SERVED_KEY: &str = "x-gateway-destination-endpoint-served";

fn overwrite(key: &str, value: &str) -> HeaderValueOption {
    HeaderValueOption {
        append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
        header: Some(HeaderValue {
            key: key.into(),
            raw_value: value.as_bytes().to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn picked(decision: &RoutingDecision, token: &str) -> ProcessingResponse {
    let endpoint = decision.endpoint.to_string();
    ProcessingResponse {
        response: Some(Response::RequestHeaders(HeadersResponse {
            response: Some(CommonResponse {
                clear_route_cache: true,
                header_mutation: Some(HeaderMutation {
                    set_headers: vec![
                        overwrite(PICK_KEY, &endpoint),
                        overwrite("x-mini-router-token", token),
                    ],
                    remove_headers: vec!["content-length".into()],
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

pub fn served(metadata: &HashMap<String, Struct>) -> Result<Option<WorkerKey>, CandidateError> {
    let Some(lb) = metadata.get(PICK_NAMESPACE) else {
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

/// Full-duplex bodies must be returned explicitly, including unmodified bytes.
/// Keep each protobuf message below the common 64 KiB gateway frame limit.
pub fn bodies(
    body: &[u8],
    end: bool,
    request: bool,
) -> impl Iterator<Item = ProcessingResponse> + '_ {
    const CHUNK: usize = 62_000;
    let count = body.len().div_ceil(CHUNK).max(1);
    (0..count).map(move |index| {
        let start = index * CHUNK;
        let response = BodyResponse {
            response: Some(CommonResponse {
                body_mutation: Some(BodyMutation {
                    mutation: Some(Mutation::StreamedResponse(StreamedBodyResponse {
                        body: body[start..body.len().min(start + CHUNK)].to_vec(),
                        end_of_stream: end && index + 1 == count,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }),
        };
        ProcessingResponse {
            response: Some(if request {
                Response::RequestBody(response)
            } else {
                Response::ResponseBody(response)
            }),
            ..Default::default()
        }
    })
}

pub fn headers() -> ProcessingResponse {
    ProcessingResponse {
        response: Some(Response::ResponseHeaders(HeadersResponse {
            response: Some(CommonResponse::default()),
        })),
        ..Default::default()
    }
}

pub fn trailers(request: bool) -> ProcessingResponse {
    let response = TrailersResponse {
        header_mutation: None,
    };
    ProcessingResponse {
        response: Some(if request {
            Response::RequestTrailers(response)
        } else {
            Response::ResponseTrailers(response)
        }),
        ..Default::default()
    }
}
