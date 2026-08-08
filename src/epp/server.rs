use crate::epp::candidates::parse;
use crate::epp::response;
use crate::routing::service::RoutingService;
use crate::types::{CandidateSet, RoutingRequest};
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_request::Request;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response as Resp;
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
        let service = self.service.clone();
        let max_body = self.max_body_bytes;
        let max_candidates = self.max_candidates;
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            let cont = || CommonResponse {
                status: ResponseStatus::Continue as i32,
                ..Default::default()
            };
            let mut chunks: Vec<u8> = Vec::new();
            let mut candidates: Option<CandidateSet> = None;
            let mut request_id: Option<String> = None;
            let mut decision: Option<Uuid> = None;

            while let Ok(Some(msg)) = stream.message().await {
                match msg.request {
                    Some(Request::RequestHeaders(headers)) => {
                        request_id = Some(
                            headers
                                .headers
                                .as_ref()
                                .and_then(|h| {
                                    h.headers
                                        .iter()
                                        .find(|v| v.key.to_lowercase() == "x-request-id")
                                })
                                .map(|v| v.value.clone())
                                .filter(|v| v.len() <= 128)
                                .unwrap_or_else(|| Uuid::new_v4().to_string()),
                        );

                        if let Some(meta) = &msg.metadata_context
                            && let Some(v) = meta.filter_metadata.values().next()
                        {
                            match parse(v, max_candidates) {
                                Ok(parsed) => candidates = Some(parsed),
                                Err(e) => {
                                    let _ =
                                        tx.send(Ok(response::unavailable(&e.to_string()))).await;
                                    let _ =
                                        tx.send(Err(Status::invalid_argument(e.to_string()))).await;
                                    return;
                                }
                            }
                        }

                        let _ = tx
                            .send(Ok(ProcessingResponse {
                                response: Some(Resp::RequestHeaders(HeadersResponse {
                                    response: Some(cont()),
                                })),
                                ..Default::default()
                            }))
                            .await;
                    }
                    Some(Request::RequestBody(body)) => {
                        chunks.extend_from_slice(&body.body);
                        if chunks.len() > max_body {
                            let _ = tx.send(Ok(response::unavailable("body too large"))).await;
                            return;
                        }
                        if !body.end_of_stream {
                            let _ = tx.send(Ok(ProcessingResponse::default())).await;
                            continue;
                        }

                        let body: serde_json::Value = match serde_json::from_slice(&chunks) {
                            Ok(v) => v,
                            Err(e) => {
                                let _ = tx.send(Ok(response::unavailable(&e.to_string()))).await;
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
                            Ok(d) => {
                                decision = Some(d.decision_id);
                                let _ = tx.send(Ok(response::picked(&d))).await;
                            }
                            Err(e) => {
                                let _ = tx.send(Ok(response::unavailable(&e.to_string()))).await;
                            }
                        }
                    }
                    Some(Request::ResponseHeaders(_)) => {
                        if let Some(meta) = &msg.metadata_context
                            && let Some(v) = meta.filter_metadata.values().next()
                            && let Ok(Some(served)) = response::served(v)
                            && let Some(d) = decision
                        {
                            let _ = service.complete_served(d, served).await;
                        }
                        let _ = tx
                            .send(Ok(ProcessingResponse {
                                response: Some(Resp::ResponseHeaders(HeadersResponse {
                                    response: Some(cont()),
                                })),
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
