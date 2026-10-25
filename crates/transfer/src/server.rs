use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::engine::Engine;
use crate::error::Error;
use crate::protocol;

impl Engine {
    pub async fn serve(&self, listener: TcpListener, stop: CancellationToken) -> Result<(), Error> {
        let mut tasks = JoinSet::new();
        let result = loop {
            tokio::select! {
                _ = stop.cancelled() => break Ok(()),
                _ = tasks.join_next(), if !tasks.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(peer) => peer,
                        Err(error) => break Err(Error::Io(error)),
                    };
                    let Ok(permit) = self.inner.peers.clone().try_acquire_owned() else {
                        continue;
                    };
                    let engine = self.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _ = handle(engine, stream).await;
                    });
                }
            }
        };
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    }
}

async fn handle(engine: Engine, mut stream: TcpStream) -> Result<(), Error> {
    stream.set_nodelay(true)?;
    let timeout = engine.inner.config.timeout;
    let handshake = tokio::time::timeout(
        timeout,
        protocol::recv(&mut stream, protocol::HANDSHAKELIMIT),
    )
    .await
    .map_err(|_| Error::Disconnected)??;
    if !protocol::authenticated(&handshake, &engine.inner.config.token) {
        protocol::send(&mut stream, &[0]).await?;
        return Err(Error::Auth);
    }
    protocol::send(&mut stream, &[1]).await?;

    let max = engine.inner.config.frame + protocol::OVERHEAD;
    loop {
        // Reserve before receiving a payload. Each connection holds at most
        // one request and response; no detached writer queue retains buffers.
        let _permit = engine
            .inner
            .inbound
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        tokio::time::timeout(timeout, async {
            let request = protocol::Request::decode(protocol::recv(&mut stream, max).await?)?;
            let frame = match engine.execute(&request) {
                Ok(body) => protocol::Response::encode(request.id, 0, &body),
                Err(error) => {
                    protocol::Response::encode(request.id, 1, error.to_string().as_bytes())
                }
            };
            protocol::send(&mut stream, &frame).await
        })
        .await
        .map_err(|_| Error::Disconnected)??;
    }
}
