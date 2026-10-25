use extproc::envoy::config::core::v3::{HeaderMap, HeaderValue, Metadata};
use extproc::envoy::service::ext_proc::v3::{
    HttpBody, HttpHeaders, HttpTrailers, ProcessingRequest, ProcessingResponse,
    ProtocolConfiguration, body_mutation::Mutation,
    external_processor_client::ExternalProcessorClient,
    external_processor_server::ExternalProcessorServer, processing_request::Request,
    processing_response::Response,
};
use kube::runtime::watcher::Event;
use prost_types::{ListValue, Struct, Value, value::Kind};
use router::config::RouterConfig;
use router::epp::server::EppServer;
use router::kv::index::PrefixIndex;
use router::renderer::client::{RenderError, Renderer};
use router::routing::service::RoutingService;
use router::telemetry::RouterMetrics;
use router::types::{RenderedInput, RendererIdentity};
use router::workers::{discovery::Discovery, manager::EndpointManager};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tonic::transport::{Channel, Server};

struct FakeRenderer(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl Renderer for FakeRenderer {
    async fn render(&self, _: &serde_json::Value) -> Result<RenderedInput, RenderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(RenderedInput {
            token_ids: Arc::from([1, 2, 3]),
        })
    }

    async fn ready(&self, _: &RendererIdentity) -> Result<(), RenderError> {
        Ok(())
    }
}

struct Harness {
    client: ExternalProcessorClient<Channel>,
    discovery: Arc<Discovery>,
    metrics: Arc<RouterMetrics>,
    renders: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Harness {
    async fn new(ready: bool, limit: usize) -> Self {
        let mut config = RouterConfig::fixture();
        config.max_body_bytes = limit;
        let renders = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(Mutex::new(PrefixIndex::default()));
        let (tx, _rx) = mpsc::channel(16);
        let metrics = Arc::new(RouterMetrics::new().unwrap());
        let manager = Arc::new(EndpointManager::new(
            reqwest::Client::new(),
            config.clone(),
            index.clone(),
            tx,
            metrics.clone(),
        ));
        let service = Arc::new(RoutingService::new(
            Arc::new(FakeRenderer(renders.clone())),
            index,
            manager,
            config,
            metrics.clone(),
        ));
        let discovery = Arc::new(Discovery::default());
        if ready {
            discovery.pool(Event::InitApply(serde_json::from_value(json!({
                "apiVersion": "inference.networking.k8s.io/v1", "kind": "InferencePool",
                "metadata": {"name": "router"},
                "spec": {"selector": {"matchLabels": {"app": "worker"}}, "targetPorts": [{"number": 8200}]}
            })).unwrap()));
            discovery.pool(Event::InitDone);
            discovery.pod(Event::InitApply(serde_json::from_value(json!({
                "metadata": {"name": "worker", "labels": {"app": "worker"}},
                "status": {"phase": "Running", "podIP": "10.0.0.1", "conditions": [{"type": "Ready", "status": "True"}]}
            })).unwrap()));
            discovery.pod(Event::InitDone);
        }
        let epp = EppServer::new(service, limit, 8, "token".into(), discovery.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(ExternalProcessorServer::new(epp))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = ExternalProcessorClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            client,
            discovery,
            metrics,
            renders,
            task,
        }
    }

    async fn stream(
        &mut self,
    ) -> (
        mpsc::Sender<ProcessingRequest>,
        tonic::Streaming<ProcessingResponse>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let response = self
            .client
            .process(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
        (tx, response.into_inner())
    }
}

fn headers(subset: bool) -> ProcessingRequest {
    ProcessingRequest {
        request: Some(Request::RequestHeaders(HttpHeaders {
            headers: Some(HeaderMap {
                headers: vec![HeaderValue {
                    key: "x-request-id".into(),
                    raw_value: b"request-one".to_vec(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })),
        protocol_config: Some(ProtocolConfiguration {
            request_body_mode: 4,
            response_body_mode: 4,
            ..Default::default()
        }),
        metadata_context: subset.then(|| Metadata {
            filter_metadata: HashMap::from([(
                "envoy.lb.subset_hint".into(),
                Struct {
                    fields: BTreeMap::from([(
                        "x-gateway-destination-endpoint-subset".into(),
                        Value {
                            kind: Some(Kind::ListValue(ListValue {
                                values: vec![Value {
                                    kind: Some(Kind::StringValue("10.0.0.1:8200".into())),
                                }],
                            })),
                        },
                    )]),
                },
            )]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn body(bytes: &[u8], end: bool, request: bool) -> ProcessingRequest {
    let body = HttpBody {
        body: bytes.to_vec(),
        end_of_stream: end,
        ..Default::default()
    };
    ProcessingRequest {
        request: Some(if request {
            Request::RequestBody(body)
        } else {
            Request::ResponseBody(body)
        }),
        ..Default::default()
    }
}

fn chat() -> Vec<u8> {
    serde_json::to_vec(
        &json!({"model": "fixed", "messages": [{"role": "user", "content": "hello"}]}),
    )
    .unwrap()
}

async fn receive(stream: &mut tonic::Streaming<ProcessingResponse>) -> Response {
    tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .response
        .unwrap()
}

async fn chunks(
    stream: &mut tonic::Streaming<ProcessingResponse>,
    request: bool,
    end: bool,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let body = match receive(stream).await {
            Response::RequestBody(body) if request => body,
            Response::ResponseBody(body) if !request => body,
            _ => panic!("expected body response"),
        };
        let Some(Mutation::StreamedResponse(chunk)) =
            body.response.unwrap().body_mutation.unwrap().mutation
        else {
            panic!("expected full-duplex body mutation");
        };
        assert!(chunk.body.len() <= 62_000);
        bytes.extend_from_slice(&chunk.body);
        if chunk.end_of_stream || !end {
            break;
        }
    }
    bytes
}

async fn no_reply(stream: &mut tonic::Streaming<ProcessingResponse>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.message())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn full_duplex_request_and_sse_preserve_order_and_bytes() {
    for subset in [false, true] {
        let mut harness = Harness::new(true, 1_000_000).await;
        let (tx, mut stream) = harness.stream().await;
        tx.send(headers(subset)).await.unwrap();
        no_reply(&mut stream).await;
        let mut input: serde_json::Value = serde_json::from_slice(&chat()).unwrap();
        input["messages"][0]["content"] = json!("a".repeat(140_000));
        input["mini_router"] = json!({"token_ids": [999]});
        input["kv_transfer_params"] = json!({"prompt_token_ids": [999]});
        let input = serde_json::to_vec(&input).unwrap();
        tx.send(body(&input[..100], false, true)).await.unwrap();
        no_reply(&mut stream).await;
        assert_eq!(harness.renders.load(Ordering::SeqCst), 0);
        tx.send(body(&input[100..], true, true)).await.unwrap();
        let Response::RequestHeaders(headers) = receive(&mut stream).await else {
            panic!("headers must precede body")
        };
        let mutation = headers.response.unwrap().header_mutation.unwrap();
        assert_eq!(
            mutation.set_headers[0].header.as_ref().unwrap().raw_value,
            b"10.0.0.1:8200"
        );
        assert_eq!(
            mutation.set_headers[1].header.as_ref().unwrap().raw_value,
            b"token"
        );
        let output: serde_json::Value =
            serde_json::from_slice(&chunks(&mut stream, true, true).await).unwrap();
        assert_eq!(output["mini_router"]["token_ids"], json!([1, 2, 3]));
        assert!(output.get("kv_transfer_params").is_none());
        assert_eq!(harness.renders.load(Ordering::SeqCst), 1);
        assert_eq!(harness.metrics.reservation_count.get(), 1.0);

        tx.send(ProcessingRequest {
            request: Some(Request::ResponseHeaders(HttpHeaders::default())),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(matches!(
            receive(&mut stream).await,
            Response::ResponseHeaders(_)
        ));
        assert_eq!(harness.metrics.reservation_count.get(), 1.0);
        for (data, end) in [
            (b"data: {\"cho".as_slice(), false),
            (b"ices\":[]}\n\ndata: [DONE]\n\n".as_slice(), true),
        ] {
            tx.send(body(data, end, false)).await.unwrap();
            assert_eq!(chunks(&mut stream, false, end).await, data);
            assert_eq!(harness.metrics.reservation_count.get(), 0.0);
        }
        assert!(stream.message().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn trailers_complete_request_and_response() {
    let mut harness = Harness::new(true, 10_000).await;
    let (tx, mut stream) = harness.stream().await;
    tx.send(headers(false)).await.unwrap();
    tx.send(body(&chat(), false, true)).await.unwrap();
    no_reply(&mut stream).await;
    tx.send(ProcessingRequest {
        request: Some(Request::RequestTrailers(HttpTrailers::default())),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(matches!(
        receive(&mut stream).await,
        Response::RequestHeaders(_)
    ));
    let output = chunks(&mut stream, true, false).await;
    assert!(serde_json::from_slice::<serde_json::Value>(&output).is_ok());
    assert!(matches!(
        receive(&mut stream).await,
        Response::RequestTrailers(_)
    ));
    tx.send(ProcessingRequest {
        request: Some(Request::ResponseHeaders(HttpHeaders::default())),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(matches!(
        receive(&mut stream).await,
        Response::ResponseHeaders(_)
    ));
    tx.send(body(b"", false, false)).await.unwrap();
    assert!(chunks(&mut stream, false, false).await.is_empty());
    assert_eq!(harness.metrics.reservation_count.get(), 1.0);
    tx.send(ProcessingRequest {
        request: Some(Request::ResponseTrailers(HttpTrailers::default())),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(matches!(
        receive(&mut stream).await,
        Response::ResponseTrailers(_)
    ));
    assert_eq!(harness.metrics.reservation_count.get(), 0.0);
}

#[tokio::test]
async fn rejected_requests_do_not_render_or_reserve() {
    for case in [
        "unsynced", "removed", "oversize", "json", "order", "protocol", "empty",
    ] {
        let mut harness = Harness::new(case != "unsynced", 100).await;
        let (tx, mut stream) = harness.stream().await;
        let mut header = headers(false);
        if case == "protocol" {
            header.protocol_config.as_mut().unwrap().request_body_mode = 1;
        }
        if case == "empty"
            && let Some(Request::RequestHeaders(headers)) = &mut header.request
        {
            headers.end_of_stream = true;
        }
        if case == "order" {
            tx.send(body(&chat(), true, true)).await.unwrap();
        } else {
            tx.send(header).await.unwrap();
        }
        if matches!(case, "removed" | "oversize" | "json") {
            no_reply(&mut stream).await;
            if case == "removed" {
                harness.discovery.pod(Event::Init);
            }
            let data = match case {
                "oversize" => vec![b'x'; 101],
                "json" => b"{".to_vec(),
                _ => chat(),
            };
            tx.send(body(&data, true, true)).await.unwrap();
        }
        let Response::ImmediateResponse(response) = receive(&mut stream).await else {
            panic!("expected rejection for {case}")
        };
        assert_eq!(response.status.unwrap().code, 503);
        assert_eq!(harness.renders.load(Ordering::SeqCst), 0);
        assert_eq!(harness.metrics.reservation_count.get(), 0.0);
    }
}

#[tokio::test]
async fn disconnect_and_invalid_order_release_reservations() {
    for case in ["half-close", "disconnect", "order", "headers-only"] {
        let mut harness = Harness::new(true, 10_000).await;
        let (tx, mut stream) = harness.stream().await;
        tx.send(headers(false)).await.unwrap();
        tx.send(body(&chat(), true, true)).await.unwrap();
        assert!(matches!(
            receive(&mut stream).await,
            Response::RequestHeaders(_)
        ));
        chunks(&mut stream, true, true).await;
        assert_eq!(harness.metrics.reservation_count.get(), 1.0);
        match case {
            "half-close" => {
                drop(tx);
                assert!(stream.message().await.unwrap().is_none());
            }
            "disconnect" => {
                drop(tx);
                drop(stream);
            }
            "order" => {
                tx.send(body(b"bad ordering", true, false)).await.unwrap();
                assert!(matches!(
                    receive(&mut stream).await,
                    Response::ImmediateResponse(_)
                ));
            }
            _ => {
                tx.send(ProcessingRequest {
                    request: Some(Request::ResponseHeaders(HttpHeaders {
                        end_of_stream: true,
                        ..Default::default()
                    })),
                    ..Default::default()
                })
                .await
                .unwrap();
                assert!(matches!(
                    receive(&mut stream).await,
                    Response::ResponseHeaders(_)
                ));
            }
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while harness.metrics.reservation_count.get() != 0.0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("reservation not released after {case}"));
    }
}
