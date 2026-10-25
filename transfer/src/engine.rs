use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Kind};
use crate::protocol::{self, Request};
use crate::region::{Access, Guard, MemoryRegion, Permissions, Region, RegionId, RemoteRegion};
use crate::sync::lock;
use crate::transport::{TcpTransport, Transport};

static NEXTENGINE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum number of in-flight transfers, also bounding accepted server requests.
    pub limit: usize,
    /// Execution deadline. Completion waits for submitted work to drain.
    pub timeout: Duration,
    /// Maximum transfer payload, also bounding protocol frames.
    pub frame: usize,
    /// Shared identity token required by the TCP server.
    pub token: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Read,
    Write,
}

impl Direction {
    pub(crate) fn local(self) -> Access {
        match self {
            Self::Read => Access::Write,
            Self::Write => Access::Read,
        }
    }

    pub(crate) fn remote(self) -> Access {
        match self {
            Self::Read => Access::Read,
            Self::Write => Access::Write,
        }
    }
}

#[derive(Clone)]
pub struct Transfer {
    pub direction: Direction,
    pub region: MemoryRegion,
    pub offset: usize,
    pub remote: RemoteRegion,
    pub remote_offset: u64,
    pub len: usize,
}

/// A terminal result delivered exactly once per [`Handle`].
#[derive(Debug)]
pub enum Completion {
    Success { bytes: usize },
    Failed(Error),
    TimedOut,
    Cancelled,
}

pub struct Handle {
    reply: Option<oneshot::Receiver<Completion>>,
    cancel: CancellationToken,
}

impl Handle {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn wait(mut self) -> Completion {
        match self.reply.take() {
            Some(reply) => reply
                .await
                .unwrap_or(Completion::Failed(Error::Disconnected)),
            None => Completion::Failed(Error::Disconnected),
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Registers local memory and submits transfers.
#[derive(Clone)]
pub struct Engine {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) id: u64,
    pub(crate) config: Config,
    pub(crate) regions: Mutex<HashMap<RegionId, Arc<Region>>>,
    pub(crate) slots: Arc<Semaphore>,
    pub(crate) inbound: Arc<Semaphore>,
    pub(crate) peers: Arc<Semaphore>,
    tainted: Mutex<HashSet<RegionId>>,
    transport: Arc<dyn Transport>,
}

impl Engine {
    pub fn new(config: Config) -> Result<Self, Error> {
        let transport = TcpTransport::new(config.token.clone(), config.frame, config.timeout);
        Self::with(config, transport)
    }

    pub fn with(config: Config, transport: impl Transport + 'static) -> Result<Self, Error> {
        if config.limit == 0 || config.limit > Semaphore::MAX_PERMITS {
            return Err(Error::Config("limit"));
        }
        if config.frame == 0 || config.frame > protocol::MAXFRAME {
            return Err(Error::Config("frame"));
        }
        if config.timeout.is_zero() {
            return Err(Error::Config("timeout"));
        }
        if config.token.is_empty()
            || config.token.len() > protocol::HANDSHAKELIMIT - protocol::PREFIX
        {
            return Err(Error::Config("token"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                id: NEXTENGINE.fetch_add(1, Ordering::Relaxed),
                slots: Arc::new(Semaphore::new(config.limit)),
                inbound: Arc::new(Semaphore::new(config.limit)),
                peers: Arc::new(Semaphore::new(config.limit)),
                tainted: Mutex::new(HashSet::new()),
                config,
                regions: Mutex::new(HashMap::new()),
                transport: Arc::new(transport),
            }),
        })
    }

    pub fn kind(&self) -> Kind {
        self.inner.transport.kind()
    }

    pub fn register(&self, data: Vec<u8>, permissions: Permissions) -> MemoryRegion {
        let region = MemoryRegion::new(self.inner.id, data, permissions);
        lock(&self.inner.regions).insert(region.id(), region.inner().clone());
        region
    }

    pub async fn deregister(&self, region: &MemoryRegion) -> Result<(), Error> {
        if region.inner().owner != self.inner.id {
            return Err(Error::Region);
        }
        {
            let mut regions = lock(&self.inner.regions);
            if let Some(current) = regions.get(&region.id()) {
                if !Arc::ptr_eq(current, region.inner()) {
                    return Err(Error::Region);
                }
                regions.remove(&region.id());
            }
        }
        region.inner().close();
        region.inner().wait().await;
        Ok(())
    }

    pub async fn read(
        &self,
        remote: RemoteRegion,
        remote_offset: u64,
        region: impl Into<MemoryRegion>,
        offset: usize,
        len: usize,
    ) -> Result<Handle, Error> {
        self.submit(Transfer {
            direction: Direction::Read,
            region: region.into(),
            offset,
            remote,
            remote_offset,
            len,
        })
        .await
    }

    pub async fn write(
        &self,
        region: impl Into<MemoryRegion>,
        offset: usize,
        remote: RemoteRegion,
        remote_offset: u64,
        len: usize,
    ) -> Result<Handle, Error> {
        self.submit(Transfer {
            direction: Direction::Write,
            region: region.into(),
            offset,
            remote,
            remote_offset,
            len,
        })
        .await
    }

    pub async fn submit(&self, transfer: Transfer) -> Result<Handle, Error> {
        self.check(&transfer)?;
        let (transfer, guard, slot) = self.ready(transfer).await?;
        Ok(self.spawn(transfer, guard, slot))
    }

    pub async fn batch(&self, transfers: Vec<Transfer>) -> Result<Vec<Handle>, Error> {
        for transfer in &transfers {
            self.check(transfer)?;
        }
        let mut handles = Vec::with_capacity(transfers.len());
        for transfer in transfers {
            let (transfer, guard, slot) = self.ready(transfer).await?;
            handles.push(self.spawn(transfer, guard, slot));
        }
        Ok(handles)
    }

    async fn ready(
        &self,
        transfer: Transfer,
    ) -> Result<(Transfer, Guard, OwnedSemaphorePermit), Error> {
        let slot = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let guard = transfer
            .region
            .inner()
            .acquire(transfer.direction.local())?;
        Ok((transfer, guard, slot))
    }

    fn check(&self, transfer: &Transfer) -> Result<(), Error> {
        if lock(&self.inner.tainted).contains(&transfer.remote.id) {
            return Err(Error::Quarantined);
        }
        if transfer.len > self.inner.config.frame {
            return Err(Error::Size);
        }
        if transfer.region.inner().owner != self.inner.id {
            return Err(Error::Region);
        }
        transfer
            .region
            .inner()
            .check(transfer.direction.local(), transfer.offset, transfer.len)?;
        u64::try_from(transfer.len)
            .ok()
            .and_then(|len| transfer.remote_offset.checked_add(len))
            .ok_or(Error::Bounds)?;
        Ok(())
    }

    fn spawn(&self, transfer: Transfer, guard: Guard, slot: OwnedSemaphorePermit) -> Handle {
        let (reply, answer) = oneshot::channel();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let engine = self.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let _guard = guard;
            // Keep the operation alive after cancellation. Its resources cannot
            // be reused until the backend has finished accessing them.
            let operation = engine.run(&transfer, &token);
            tokio::pin!(operation);
            let outcome = tokio::select! {
                biased;
                _ = token.cancelled() => {
                    match operation.await {
                        Ok(_) => Completion::Cancelled,
                        Err(error) => Completion::Failed(error),
                    }
                },
                result = tokio::time::timeout(engine.inner.config.timeout, &mut operation) => {
                    match result {
                        Ok(Ok(bytes)) => Completion::Success { bytes },
                        Ok(Err(error)) => Completion::Failed(error),
                        Err(_) => {
                            token.cancel();
                            match operation.await {
                                Ok(_) => Completion::TimedOut,
                                Err(error) => Completion::Failed(error),
                            }
                        },
                    }
                }
            };
            let _ = reply.send(outcome);
        });
        Handle {
            reply: Some(answer),
            cancel,
        }
    }

    async fn run(&self, transfer: &Transfer, cancel: &CancellationToken) -> Result<usize, Error> {
        if cancel.is_cancelled() {
            return Ok(0);
        }
        if lock(&self.inner.tainted).contains(&transfer.remote.id) {
            return Err(Error::Quarantined);
        }
        match transfer.direction {
            Direction::Read => {
                let data = self
                    .inner
                    .transport
                    .read(transfer.remote, transfer.remote_offset, transfer.len)
                    .await?;
                if data.len() != transfer.len {
                    return Err(Error::Protocol("response length mismatch".into()));
                }
                if !cancel.is_cancelled() {
                    transfer.region.inner().write(transfer.offset, &data)?;
                }
            }
            Direction::Write => {
                let data = transfer
                    .region
                    .inner()
                    .read(transfer.offset, transfer.len)?;
                self.inner
                    .transport
                    .write(transfer.remote, transfer.remote_offset, data)
                    .await
                    .map_err(|source| {
                        lock(&self.inner.tainted).insert(transfer.remote.id);
                        Error::Uncertain {
                            remote: transfer.remote,
                            source: Box::new(source),
                        }
                    })?;
            }
        }
        Ok(transfer.len)
    }

    pub(crate) fn region(&self, id: RegionId) -> Result<Arc<Region>, Error> {
        lock(&self.inner.regions)
            .get(&id)
            .cloned()
            .ok_or(Error::Region)
    }

    pub(crate) fn execute(&self, request: &Request) -> Result<Vec<u8>, Error> {
        if request.len > self.inner.config.frame {
            return Err(Error::Size);
        }
        let offset = usize::try_from(request.offset).map_err(|_| Error::Bounds)?;
        let region = self.region(request.region)?;
        let _guard = region.acquire(request.direction.remote())?;
        match request.direction {
            Direction::Read => region.read(offset, request.len),
            Direction::Write => {
                region.write(offset, &request.data)?;
                Ok(Vec::new())
            }
        }
    }
}
