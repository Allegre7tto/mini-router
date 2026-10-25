use extproc::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use router::config::RouterConfig;
use router::epp::server::EppServer;
use router::kv::index::PrefixIndex;
use router::kv::subscriber::Event;
use router::renderer::client::{HttpRenderer, Renderer};
use router::routing::service::RoutingService;
use router::telemetry::RouterMetrics;
use router::types::{RendererIdentity, WorkerKey};
use router::workers::discovery::Discovery;
use router::workers::manager::EndpointManager;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use tonic::transport::{Identity, Server, ServerTlsConfig};

type Processor = ExternalProcessorServer<EppServer>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = RouterConfig::env()?;
    let client = kube::Client::try_default().await?;
    let discovery = Arc::new(Discovery::default());
    let membership = tokio::spawn({
        let discovery = discovery.clone();
        let namespace = config.namespace.clone();
        let pool = config.pool.clone();
        async move { discovery.run(client, &namespace, &pool).await }
    });

    let renderer = Arc::new(HttpRenderer::new(
        config.renderer_url.clone(),
        Duration::from_secs_f64(config.renderer_timeout_secs),
        config.max_token_count,
    ));
    let (tx, mut rx) = mpsc::channel::<(WorkerKey, Event)>(1024);
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let metrics = Arc::new(RouterMetrics::new()?);
    let app = axum::Router::new()
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route(
            "/metrics",
            axum::routing::get({
                let metrics = metrics.clone();
                move || {
                    let metrics = metrics.clone();
                    async move {
                        metrics
                            .encode()
                            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind(&config.metrics_listen).await?;
    let monitor = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!("metrics server failed: {error}");
        }
    });
    let manager = Arc::new(EndpointManager::new(
        reqwest::Client::new(),
        config.clone(),
        index.clone(),
        tx,
        metrics.clone(),
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
                manager.refresh().await;
                manager.reap(Instant::now()).await;
            }
        })
    };

    let service = Arc::new(RoutingService::new(
        renderer.clone(),
        index,
        manager,
        config.clone(),
        metrics,
    ));

    let epp = EppServer::new(
        service,
        config.max_body_bytes,
        config.max_candidates,
        config.workload_token.clone(),
        discovery.clone(),
    );
    let svc = Processor::new(epp);
    let (health, probe) = tonic_health::server::health_reporter();
    health.set_not_serving::<Processor>().await;
    let readiness = tokio::spawn({
        let identity = RendererIdentity {
            model: config.model.clone(),
            revision: config.revision.clone(),
            probe_digest: config.probe_digest.clone(),
        };
        async move {
            loop {
                match renderer.ready(&identity).await {
                    Ok(()) if discovery.ready() => health.set_serving::<Processor>().await,
                    Ok(()) => health.set_not_serving::<Processor>().await,
                    Err(error) => {
                        tracing::warn!(%error, "renderer readiness check failed");
                        health.set_not_serving::<Processor>().await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(config.metrics_interval_secs)).await;
            }
        }
    });

    let addr = config.epp_listen.parse()?;
    tracing::info!("EPP server listening on {addr}");

    let mut server = Server::builder();
    if let (Some(cert), Some(key)) = (&config.cert, &config.key) {
        let identity =
            Identity::from_pem(tokio::fs::read(cert).await?, tokio::fs::read(key).await?);
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }
    let serve = server
        .add_service(probe.clone())
        .add_service(svc)
        .serve(addr);
    // Kubelet gRPC probes require plaintext, separate from the TLS EPP listener.
    let probes = Server::builder()
        .add_service(probe)
        .serve(config.health.parse()?);
    let result = tokio::try_join!(serve, probes);
    for task in [membership, consumer, janitor, monitor, readiness] {
        task.abort();
    }
    result?;
    Ok(())
}
