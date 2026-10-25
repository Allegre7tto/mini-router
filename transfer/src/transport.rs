use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::engine::Direction;
use crate::error::{Error, Kind};
use crate::protocol;
use crate::region::{RegionId, RemoteRegion};
use crate::sync::lock;

/// Backend boundary: the runtime depends on this trait, not on TCP or RDMA.
#[async_trait]
pub trait Transport: Send + Sync {
    fn kind(&self) -> Kind;

    async fn read(&self, remote: RemoteRegion, offset: u64, len: usize) -> Result<Vec<u8>, Error>;

    async fn write(&self, remote: RemoteRegion, offset: u64, data: Vec<u8>) -> Result<(), Error>;
}

pub(crate) struct TcpTransport {
    token: Arc<[u8]>,
    frame: usize,
    max: usize,
    timeout: Duration,
    connections: tokio::sync::Mutex<HashMap<SocketAddr, Arc<Connection>>>,
}

impl TcpTransport {
    pub(crate) fn new(token: Vec<u8>, frame: usize, timeout: Duration) -> Self {
        Self {
            token: token.into(),
            frame,
            max: frame + protocol::OVERHEAD,
            timeout,
            connections: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn connection(&self, endpoint: SocketAddr) -> Result<Arc<Connection>, Error> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(&endpoint)
            && !connection.dead()
        {
            return Ok(connection.clone());
        }
        let connection = Arc::new(Connection::connect(endpoint, &self.token, self.max).await?);
        connections.insert(endpoint, connection.clone());
        Ok(connection)
    }

    async fn request(
        &self,
        direction: Direction,
        remote: RemoteRegion,
        offset: u64,
        len: usize,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        // Allow a bounded drain interval after the engine's deadline. Closing
        // the socket still cannot prove a remote write did not commit.
        let timeout = self.timeout.saturating_mul(2);
        let connection = tokio::time::timeout(timeout, self.connection(remote.endpoint))
            .await
            .map_err(|_| Error::Disconnected)??;
        match tokio::time::timeout(
            timeout,
            connection.request(direction, remote.id, offset, len, data),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                connection.close();
                Err(Error::Disconnected)
            }
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    fn kind(&self) -> Kind {
        Kind::Tcp
    }

    async fn read(&self, remote: RemoteRegion, offset: u64, len: usize) -> Result<Vec<u8>, Error> {
        if len > self.frame {
            return Err(Error::Size);
        }
        self.request(Direction::Read, remote, offset, len, Vec::new())
            .await
            .map_err(|error| error.tcp(true))
    }

    async fn write(&self, remote: RemoteRegion, offset: u64, data: Vec<u8>) -> Result<(), Error> {
        let len = data.len();
        if len > self.frame {
            return Err(Error::Size);
        }
        self.request(Direction::Write, remote, offset, len, data)
            .await
            .map(|_| ())
            .map_err(|error| error.tcp(false))
    }
}

type Reply = Result<(u8, Vec<u8>), Error>;

struct Shared {
    out: mpsc::Sender<Vec<u8>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Reply>>>,
    next: AtomicU64,
    dead: AtomicBool,
}

impl Shared {
    fn fail(&self) {
        if self.dead.swap(true, Ordering::SeqCst) {
            return;
        }
        let pending: Vec<_> = lock(&self.pending).drain().map(|(_, tx)| tx).collect();
        for tx in pending {
            let _ = tx.send(Err(Error::Disconnected));
        }
    }
}

struct Connection {
    shared: Arc<Shared>,
    tasks: [AbortHandle; 2],
}

impl Connection {
    async fn connect(endpoint: SocketAddr, token: &[u8], max: usize) -> Result<Self, Error> {
        let mut stream = TcpStream::connect(endpoint).await?;
        stream.set_nodelay(true)?;
        protocol::send(&mut stream, &protocol::handshake(token)).await?;
        if protocol::recv(&mut stream, protocol::HANDSHAKELIMIT)
            .await?
            .first()
            != Some(&1)
        {
            return Err(Error::Auth);
        }

        let (reader, writer) = stream.into_split();
        let (out, frames) = mpsc::channel(protocol::QUEUE);
        let shared = Arc::new(Shared {
            out,
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            dead: AtomicBool::new(false),
        });

        let reading = {
            let shared = shared.clone();
            tokio::spawn(dispatch(reader, max, shared))
        };
        let writing = {
            let shared = shared.clone();
            tokio::spawn(async move {
                if !protocol::pump(writer, frames).await {
                    shared.fail();
                }
            })
        };

        Ok(Self {
            shared,
            tasks: [reading.abort_handle(), writing.abort_handle()],
        })
    }

    fn dead(&self) -> bool {
        self.shared.dead.load(Ordering::SeqCst)
    }

    fn close(&self) {
        for task in &self.tasks {
            task.abort();
        }
        self.shared.fail();
    }

    async fn request(
        &self,
        direction: Direction,
        region: RegionId,
        offset: u64,
        len: usize,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        if self.dead() {
            return Err(Error::Disconnected);
        }
        let id = self.shared.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = lock(&self.shared.pending);
            if self.dead() {
                return Err(Error::Disconnected);
            }
            pending.insert(id, sender);
        }
        let _pending = Pending {
            shared: &self.shared,
            id,
        };

        self.shared
            .out
            .send(
                protocol::Request {
                    id,
                    direction,
                    region,
                    offset,
                    len,
                    data,
                }
                .encode(),
            )
            .await
            .map_err(|_| Error::Disconnected)?;

        match receiver.await {
            Ok(Ok((0, body))) => match direction {
                Direction::Read if body.len() == len => Ok(body),
                Direction::Write if body.is_empty() => Ok(body),
                _ => Err(Error::Protocol("response length mismatch".into())),
            },
            Ok(Ok((_, body))) => Err(Error::Remote(String::from_utf8_lossy(&body).into_owned())),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Disconnected),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}

struct Pending<'a> {
    shared: &'a Shared,
    id: u64,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        lock(&self.shared.pending).remove(&self.id);
    }
}

async fn dispatch(mut reader: OwnedReadHalf, max: usize, shared: Arc<Shared>) {
    loop {
        let frame = match protocol::recv(&mut reader, max).await {
            Ok(frame) => frame,
            Err(_) => {
                shared.fail();
                return;
            }
        };
        let response = match protocol::Response::decode(&frame) {
            Ok(reply) => reply,
            Err(_) => {
                shared.fail();
                return;
            }
        };
        if let Some(sender) = lock(&shared.pending).remove(&response.id) {
            let _ = sender.send(Ok((response.status, response.body.to_vec())));
        }
    }
}
