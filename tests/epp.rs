use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_request::Request;
use envoy_grpc_ext_proc::envoy::service::ext_proc::v3::{HttpBody, HttpHeaders, ProcessingRequest};
use prost_types::{ListValue, Struct, Value, value::Kind};
use router::config::RouterConfig;
use router::epp::candidates::SUBSET_NAMESPACE;
use router::epp::server::EppServer;
use router::kv::index::PrefixIndex;
use router::renderer::client::Renderer;
use router::routing::service::RoutingService;
use router::workers::manager::EndpointManager;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tonic::Request as TonicRequest;
use tonic::transport::Server;

struct FakeRenderer;
#[async_trait::async_trait]
impl Renderer for FakeRenderer {
    async fn render(
        &self,
        _body: &serde_json::Value,
    ) -> Result<router::types::RenderedInput, router::renderer::client::RenderError> {
        Ok(router::types::RenderedInput {
            token_ids: Arc::from([1u32, 2, 3].as_slice()),
        })
    }
    async fn ready(&self, _model: &str) -> Result<(), router::renderer::client::RenderError> {
        Ok(())
    }
}

fn test_candidate_metadata() -> envoy_grpc_ext_proc::envoy::config::core::v3::Metadata {
    let mut subset_fields = BTreeMap::new();
    let list = ListValue {
        values: vec![Value {
            kind: Some(Kind::StringValue("10.0.0.1:8200".to_string())),
        }],
    };
    subset_fields.insert(
        "x-gateway-destination-endpoint-subset".to_string(),
        Value {
            kind: Some(Kind::ListValue(list)),
        },
    );
    let mut filter_metadata = HashMap::new();
    filter_metadata.insert(
        SUBSET_NAMESPACE.to_string(),
        Struct {
            fields: subset_fields,
        },
    );
    envoy_grpc_ext_proc::envoy::config::core::v3::Metadata {
        filter_metadata,
        ..Default::default()
    }
}

#[tokio::test]
async fn epp_request_headers_flow() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let renderer: Box<dyn Renderer> = Box::new(FakeRenderer);
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let (tx, _rx) = mpsc::channel(16);
    let manager = Arc::new(EndpointManager::new(
        client,
        config.clone(),
        index.clone(),
        tx,
    ));
    let service = Arc::new(RoutingService::new(renderer, index, manager, config));
    let epp = EppServer::new(service, 100000, 8);
    let addr: std::net::SocketAddr = "[::1]:0".parse().unwrap();
    let svc = envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer::new(epp);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://[::1]:{}", actual_addr.port()))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let mut client = ExternalProcessorClient::new(channel);

    let metadata = test_candidate_metadata();
    let headers_req = ProcessingRequest {
        request: Some(Request::RequestHeaders(HttpHeaders {
            headers: Some(envoy_grpc_ext_proc::envoy::config::core::v3::HeaderMap {
                headers: vec![envoy_grpc_ext_proc::envoy::config::core::v3::HeaderValue {
                    key: "x-request-id".into(),
                    value: "test-1".into(),
                    raw_value: vec![],
                }],
            }),
            ..Default::default()
        })),
        metadata_context: Some(metadata),
        ..Default::default()
    };

    let body_req = ProcessingRequest {
        request: Some(Request::RequestBody(HttpBody {
            body: br#"{"model":"fixed","messages":[{"role":"user","content":"hi"}]}"#.to_vec(),
            end_of_stream: true,
            ..Default::default()
        })),
        ..Default::default()
    };

    let outgoing = tokio_stream::iter(vec![headers_req, body_req]);
    let response = client.process(TonicRequest::new(outgoing)).await.unwrap();
    let mut stream = response.into_inner();
    let mut saw_headers_response = false;
    let mut saw_body_response = false;

    while let Ok(Some(msg)) = stream.message().await {
        if let Some(ep_resp) = &msg.response {
            match ep_resp {
                envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::RequestHeaders(_) => {
                    saw_headers_response = true;
                }
                envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::RequestBody(_) => {
                    saw_body_response = true;
                }
                _ => {}
            }
        }
    }
    assert!(saw_headers_response);
    assert!(saw_body_response);
}

#[tokio::test]
async fn epp_missing_candidates_returns_503() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let renderer: Box<dyn Renderer> = Box::new(FakeRenderer);
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let (tx, _rx) = mpsc::channel(16);
    let manager = Arc::new(EndpointManager::new(
        client,
        config.clone(),
        index.clone(),
        tx,
    ));
    let service = Arc::new(RoutingService::new(renderer, index, manager, config));
    let epp = EppServer::new(service, 100000, 8);
    let addr: std::net::SocketAddr = "[::1]:0".parse().unwrap();
    let svc = envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer::new(epp);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://[::1]:{}", actual_addr.port()))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let mut client = ExternalProcessorClient::new(channel);

    let headers_req = ProcessingRequest {
        request: Some(Request::RequestHeaders(HttpHeaders::default())),
        ..Default::default()
    };

    let outgoing = tokio_stream::iter(vec![headers_req]);
    let response = client.process(TonicRequest::new(outgoing)).await.unwrap();
    let mut stream = response.into_inner();
    let mut saw_503 = false;
    while let Ok(Some(msg)) = stream.message().await {
        if let Some(ep_resp) = &msg.response
            && matches!(
                ep_resp,
                envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::ImmediateResponse(_)
            )
        {
            saw_503 = true;
        }
    }
    assert!(saw_503);
}
