use crate::epp::candidates::optional;
use crate::epp::response;
use crate::routing::service::RoutingService;
use crate::types::{CandidateSet, RoutingRequest};
use crate::workers::discovery::Discovery;
use anyhow::{Context, bail};
use extproc::envoy::extensions::filters::http::ext_proc::v3::processing_mode::BodySendMode;
use extproc::envoy::service::ext_proc::v3::{
    ProcessingRequest, ProcessingResponse, external_processor_server::ExternalProcessor,
    processing_request::Request,
};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status, Streaming};
use uuid::Uuid;

#[derive(Clone)]
pub struct EppServer {
    service: Arc<RoutingService>,
    max_body_bytes: usize,
    max_candidates: usize,
    token: String,
    discovery: Arc<Discovery>,
}

impl EppServer {
    pub fn new(
        service: Arc<RoutingService>,
        max_body_bytes: usize,
        max_candidates: usize,
        token: String,
        discovery: Arc<Discovery>,
    ) -> Self {
        Self {
            service,
            max_body_bytes,
            max_candidates,
            token,
            discovery,
        }
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Headers,
    Body,
    ResponseHeaders,
    ResponseBody,
}

struct Session {
    epp: EppServer,
    phase: Phase,
    body: Vec<u8>,
    candidates: CandidateSet,
    request_id: String,
    decision: Option<Uuid>,
}

type Sender = mpsc::Sender<Result<ProcessingResponse, Status>>;

impl Session {
    async fn release(&mut self) {
        if let Some(decision) = self.decision.take() {
            self.epp.service.release(decision).await;
        }
    }

    async fn route(&mut self, tx: &Sender, end: bool) -> anyhow::Result<()> {
        let body = serde_json::from_slice::<serde_json::Value>(&std::mem::take(&mut self.body))
            .context("invalid chat body")?;
        let candidates = self
            .epp
            .discovery
            .candidates(Some(&self.candidates), self.epp.max_candidates)?;
        let decision = self
            .epp
            .service
            .route(RoutingRequest {
                request_id: self.request_id.clone(),
                body,
                candidates,
            })
            .await?;
        // Adopt the reservation before any fallible operation or channel send.
        self.decision = Some(decision.decision_id);
        if !self
            .epp
            .discovery
            .candidates(Some(&self.candidates), self.epp.max_candidates)?
            .0
            .contains(&decision.endpoint)
        {
            bail!("endpoint left InferencePool");
        }
        // Full duplex lets us receive the body before replying to its headers.
        // Set routing/auth headers first, then return the rewritten body chunks.
        tx.send(Ok(response::picked(&decision, &self.epp.token)))
            .await?;
        for chunk in response::bodies(&decision.body, end, true) {
            tx.send(Ok(chunk)).await?;
        }
        self.phase = Phase::ResponseHeaders;
        Ok(())
    }

    async fn handle(&mut self, msg: ProcessingRequest, tx: &Sender) -> anyhow::Result<bool> {
        if let Some(protocol) = &msg.protocol_config
            && (protocol.request_body_mode != BodySendMode::FullDuplexStreamed as i32
                || protocol.response_body_mode != BodySendMode::FullDuplexStreamed as i32)
        {
            bail!("EPP requires FULL_DUPLEX_STREAMED request and response bodies");
        }
        match (self.phase, msg.request.as_ref()) {
            (Phase::Headers, Some(Request::RequestHeaders(headers))) => {
                if headers.end_of_stream {
                    bail!("chat body required");
                }
                self.request_id = headers
                    .headers
                    .as_ref()
                    .and_then(|headers| {
                        headers
                            .headers
                            .iter()
                            .find(|h| h.key.eq_ignore_ascii_case("x-request-id"))
                    })
                    .map(|h| {
                        if h.raw_value.is_empty() {
                            h.value.clone()
                        } else {
                            String::from_utf8_lossy(&h.raw_value).into_owned()
                        }
                    })
                    .filter(|value| !value.is_empty() && value.len() <= 128)
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let subset = msg.metadata_context.as_ref().map_or(Ok(None), |meta| {
                    optional(&meta.filter_metadata, self.epp.max_candidates)
                })?;
                self.candidates = self
                    .epp
                    .discovery
                    .candidates(subset.as_ref(), self.epp.max_candidates)?;
                self.phase = Phase::Body;
                // Do not acknowledge headers or partial bodies before selection.
            }
            (Phase::Body, Some(Request::RequestBody(body))) => {
                if body.body.len() > self.epp.max_body_bytes.saturating_sub(self.body.len()) {
                    bail!("body too large");
                }
                self.body.extend_from_slice(&body.body);
                if body.end_of_stream {
                    self.route(tx, true).await?;
                }
            }
            (Phase::Body, Some(Request::RequestTrailers(_))) => {
                self.route(tx, false).await?;
                tx.send(Ok(response::trailers(true))).await?;
            }
            (Phase::ResponseHeaders, Some(Request::ResponseHeaders(headers))) => {
                if let Some(metadata) = &msg.metadata_context
                    && let Ok(Some(served)) = response::served(&metadata.filter_metadata)
                    && let Some(decision) = self.decision.take()
                {
                    let _ = self.epp.service.complete_served(decision, served).await;
                }
                if headers.end_of_stream {
                    self.release().await;
                }
                tx.send(Ok(response::headers())).await?;
                self.phase = Phase::ResponseBody;
                return Ok(headers.end_of_stream);
            }
            (Phase::ResponseBody, Some(Request::ResponseBody(body))) => {
                // First response bytes are a coarse prefill-completion signal.
                // We don't parse or reconstruct JSON/SSE, or track decode load.
                if !body.body.is_empty() || body.end_of_stream {
                    self.release().await;
                }
                for chunk in response::bodies(&body.body, body.end_of_stream, false) {
                    tx.send(Ok(chunk)).await?;
                }
                return Ok(body.end_of_stream);
            }
            (Phase::ResponseBody, Some(Request::ResponseTrailers(_))) => {
                self.release().await;
                tx.send(Ok(response::trailers(false))).await?;
                return Ok(true);
            }
            _ => bail!("unexpected EPP message order"),
        }
        Ok(false)
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
        let mut session = Session {
            epp: self.clone(),
            phase: Phase::Headers,
            body: Vec::new(),
            candidates: CandidateSet::default(),
            request_id: String::new(),
            decision: None,
        };
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = tx.closed() => break,
                    msg = stream.message() => match msg {
                        Ok(Some(msg)) => msg,
                        _ => break,
                    },
                };
                match session.handle(msg, &tx).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => {
                        if !tx.is_closed() {
                            let _ = tx.send(Ok(response::unavailable(&error.to_string()))).await;
                        }
                        break;
                    }
                }
            }
            // Also covers half-close, disconnect, malformed ordering, and failed sends.
            session.release().await;
        });
        Ok(TonicResponse::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}
