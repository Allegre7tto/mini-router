use router::config::RouterConfig;
use router::epp::server::EppServer;
use router::kv::index::PrefixIndex;
use router::kv::subscriber::Event;
use router::renderer::client::HttpRenderer;
use router::routing::service::RoutingService;
use router::types::WorkerKey;
use router::workers::manager::EndpointManager;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
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
        Duration::from_secs_f64(config.renderer_timeout_secs),
        config.max_token_count,
    );

    let (tx, mut rx) = mpsc::channel::<(WorkerKey, Event)>(1024);
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let manager = Arc::new(EndpointManager::new(
        reqwest::Client::new(),
        config.clone(),
        index.clone(),
        tx,
    ));

    let consumer = {
        let manager = manager.clone();
        tokio::spawn(async move {
            while let Some((worker, event)) = rx.recv().await {
                manager.handle(worker, event).await;
            }
        })
    };

    let janitor = {
        let manager = manager.clone();
        let interval = Duration::from_secs(config.metrics_interval_secs);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                manager.reap(Instant::now()).await;
            }
        })
    };

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

    let serve = Server::builder().add_service(svc).serve(addr);
    if let Err(e) = serve.await {
        consumer.abort();
        janitor.abort();
        return Err(e.into());
    }
    consumer.abort();
    janitor.abort();
    Ok(())
}
