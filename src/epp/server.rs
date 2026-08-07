#![allow(clippy::collapsible_if)]

use crate::epp::candidates::parse;
use crate::epp::response;
use crate::routing::service::{RoutingService, ServiceError};
use crate::types::RoutingRequest;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_request::Request;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::{
    CommonResponse, HeadersResponse, ProcessingRequest, ProcessingResponse,
};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status, Streaming};
use uuid::Uuid;

pub struct EppServer {
    service: Arc<RoutingService>,
    max_body_bytes: usize,
    max_candidates: usize,
}

impl EppServer {
    pub fn new(service: Arc<RoutingService>, max_body_bytes: usize, max_candidates: usize) -> Self {
        EppServer {
            service,
            max_body_bytes,
            max_candidates,
        }
    }
}

#[tonic::async_trait]
impl ExternalProcessor for EppServer {
    type ProcessStream =
        Pin<Box<dyn Stream<Item = Result<ProcessingResponse, Status>> + Send + 'static>>;

    async fn process(
        &self,
        request: TonicRequest<Streaming<ProcessingRequest>>,
    ) -> Result<TonicResponse<Self::ProcessStream>, Status> {
        let mut stream = request.into_inner();
        let mut body_chunks: Vec<u8> = Vec::new();
        let mut candidates: Option<crate::types::CandidateSet> = None;
        let mut request_id: Option<String> = None;

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let service = self.service.clone();
        let max_body = self.max_body_bytes;
        let max_cands = self.max_candidates;

        tokio::spawn(async move {
            let mut decision_id: Option<Uuid> = None;

            while let Ok(Some(msg)) = stream.message().await {
                match msg.request {
                    Some(Request::RequestHeaders(headers)) => {
                        let default_id = Uuid::new_v4().to_string();
                        request_id = Some(
                            headers
                                .headers
                                .as_ref()
                                .and_then(|h| {
                                    h.headers
                                        .iter()
                                        .find(|hv| hv.key.to_lowercase() == "x-request-id")
                                        .map(|hv| {
                                            let id = &hv.value;
                                            if id.len() <= 128 {
                                                id.clone()
                                            } else {
                                                default_id.clone()
                                            }
                                        })
                                })
                                .unwrap_or(default_id),
                        );

                        if let Some(ref meta) = msg.metadata_context {
                            if let Some(v) = meta.filter_metadata.values().next() {
                                match parse(v, max_cands) {
                                    Ok(c) => {
                                        candidates = Some(c);
                                    }
                                    Err(e) => {
                                        let _ = tx
                                            .send(Ok(response::unavailable(&e.to_string())))
                                            .await;
                                        let _ = tx
                                            .send(Err(Status::invalid_argument(e.to_string())))
                                            .await;
                                        return;
                                    }
                                }
                            }
                        }

                        let _ = tx
                            .send(Ok(ProcessingResponse {
                                response: Some(
                                    envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::RequestHeaders(HeadersResponse {
                                        response: Some(
                                            CommonResponse {
                                                status: ResponseStatus::Continue as i32,
                                                ..Default::default()
                                            },
                                        ),
                                    }),
                                ),
                                ..Default::default()
                            }))
                            .await;
                    }
                    Some(Request::RequestBody(body_msg)) => {
                        body_chunks.extend_from_slice(&body_msg.body);
                        if body_chunks.len() > max_body {
                            let _ = tx.send(Ok(response::unavailable("body too large"))).await;
                            return;
                        }
                        if body_msg.end_of_stream {
                            let body: serde_json::Value = match serde_json::from_slice(&body_chunks)
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    let _ =
                                        tx.send(Ok(response::unavailable(&e.to_string()))).await;
                                    return;
                                }
                            };

                            let req = RoutingRequest {
                                request_id: request_id.clone().unwrap_or_default(),
                                model: String::new(),
                                body,
                                candidates: candidates.clone().unwrap_or_default(),
                            };

                            match service.route(req).await {
                                Ok(decision) => {
                                    decision_id = Some(decision.decision_id);
                                    let resp = response::picked(&decision);
                                    let _ = tx.send(Ok(resp)).await;
                                }
                                Err(ServiceError::Validation(e)) => {
                                    let _ =
                                        tx.send(Ok(response::unavailable(&e.to_string()))).await;
                                }
                                Err(e) => {
                                    let _ =
                                        tx.send(Ok(response::unavailable(&e.to_string()))).await;
                                }
                            }
                        } else {
                            let _ = tx
                                .send(Ok(ProcessingResponse {
                                    ..Default::default()
                                }))
                                .await;
                        }
                    }
                    Some(Request::ResponseHeaders(_resp_hdr)) => {
                        if let Some(ref meta) = msg.metadata_context {
                            if let Some(v) = meta.filter_metadata.values().next() {
                                if let Ok(Some(served)) = response::served(v) {
                                    if let Some(did) = decision_id {
                                        let _ = service.complete_served(did, served).await;
                                    }
                                }
                            }
                        }
                        let _ = tx
                            .send(Ok(ProcessingResponse {
                                response: Some(envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::ResponseHeaders(
                                    envoy_grpc_ext_proc::envoy::service::ext_proc::v3::HeadersResponse {
                                        response: Some(CommonResponse {
                                            status: ResponseStatus::Continue as i32,
                                            ..Default::default()
                                        }),
                                    },
                                )),
                                ..Default::default()
                            }))
                            .await;
                    }
                    Some(Request::ResponseBody(_)) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "response body processing not configured",
                            )))
                            .await;
                        return;
                    }
                    _ => {}
                }
            }
        });

        let output = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(TonicResponse::new(Box::pin(output) as Self::ProcessStream))
    }
}
