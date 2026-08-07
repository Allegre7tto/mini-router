use router::config::RouterConfig;
use router::epp::server::EppServer;
use router::kv::index::PrefixIndex;
use router::renderer::client::HttpRenderer;
use router::routing::service::RoutingService;
use router::workers::manager::EndpointManager;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = RouterConfig::fixture();
    config.validate()?;

    let renderer = HttpRenderer::new(
        config.renderer_url.clone(),
        std::time::Duration::from_secs_f64(config.renderer_timeout_secs),
        config.max_token_count,
    );

    let client = reqwest::Client::new();
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let manager = Arc::new(EndpointManager::new(client, config.clone()));
    let service = Arc::new(RoutingService::new(
        Box::new(renderer),
        index,
        manager,
        config.clone(),
    ));

    let epp = EppServer::new(service, config.max_body_bytes, config.max_candidates);
    let svc = envoy_grpc_ext_proc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer::new(epp);

    let addr = config.epp_listen.parse()?;
    tracing::info!("EPP server listening on {addr}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
