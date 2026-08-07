use router::adapter::proxy::{router, AdapterState};
use router::config::AdapterConfig;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = AdapterConfig::fixture();
    config.validate()?;

    let client = reqwest::Client::builder()
        .build()
        .expect("failed to build reqwest client");

    let state = Arc::new(AdapterState {
        config: config.clone(),
        client,
    });
    let app = router(state);
    let addr: SocketAddr = config.listen.parse()?;
    tracing::info!("adapter listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
