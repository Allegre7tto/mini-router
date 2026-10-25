use std::ops::Range;
use std::sync::PoisonError;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Notify;

use crate::error::Error;
use crate::sync::lock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegionId(pub(crate) u128);

impl std::fmt::Display for RemoteRegion {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{}/{:032x}", self.endpoint, self.id.0)
    }
}

impl std::str::FromStr for RemoteRegion {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        let (endpoint, id) = text
            .split_once('/')
            .ok_or_else(|| Error::Protocol("invalid descriptor".into()))?;
        Ok(Self {
            endpoint: endpoint
                .parse()
                .map_err(|_| Error::Protocol("invalid endpoint".into()))?,
            id: RegionId(
                u128::from_str_radix(id, 16)
                    .map_err(|_| Error::Protocol("invalid region id".into()))?,
            ),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permissions(u8);

impl Permissions {
    pub const READ: Self = Self(1);
    pub const WRITE: Self = Self(2);
    pub const READWRITE: Self = Self(3);

    pub fn allows(self, access: Access) -> bool {
        let bit = match access {
            Access::Read => 1,
            Access::Write => 2,
        };
        self.0 & bit != 0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteRegion {
    pub endpoint: std::net::SocketAddr,
    pub id: RegionId,
}

pub(crate) struct Region {
    pub(crate) id: RegionId,
    pub(crate) owner: u64,
    pub(crate) permissions: Permissions,
    data: RwLock<Vec<u8>>,
    state: Mutex<State>,
    ready: Notify,
}

struct State {
    active: usize,
    open: bool,
}

pub(crate) struct Guard {
    region: Arc<Region>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = lock(&self.region.state);
        state.active -= 1;
        let idle = state.active == 0;
        drop(state);
        if idle {
            self.region.ready.notify_waiters();
        }
    }
}

impl Region {
    fn new(owner: u64, data: Vec<u8>, permissions: Permissions) -> Self {
        Self {
            id: RegionId(uuid::Uuid::new_v4().as_u128()),
            owner,
            permissions,
            data: RwLock::new(data),
            state: Mutex::new(State {
                active: 0,
                open: true,
            }),
            ready: Notify::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.data
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub(crate) fn check(&self, access: Access, offset: usize, len: usize) -> Result<(), Error> {
        let state = lock(&self.state);
        if !state.open {
            return Err(Error::Closed);
        }
        drop(state);
        if !self.permissions.allows(access) {
            return Err(Error::Permission);
        }
        let data = self.data.read().unwrap_or_else(PoisonError::into_inner);
        range(offset, len, data.len()).map(|_| ())
    }

    pub(crate) fn acquire(self: &Arc<Self>, access: Access) -> Result<Guard, Error> {
        let mut state = lock(&self.state);
        if !state.open {
            return Err(Error::Closed);
        }
        if !self.permissions.allows(access) {
            return Err(Error::Permission);
        }
        state.active += 1;
        Ok(Guard {
            region: self.clone(),
        })
    }

    pub(crate) fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>, Error> {
        let data = self.data.read().unwrap_or_else(PoisonError::into_inner);
        let range = range(offset, len, data.len())?;
        Ok(data[range].to_vec())
    }

    pub(crate) fn write(&self, offset: usize, bytes: &[u8]) -> Result<(), Error> {
        let mut data = self.data.write().unwrap_or_else(PoisonError::into_inner);
        let range = range(offset, bytes.len(), data.len())?;
        data[range].copy_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn close(&self) {
        lock(&self.state).open = false;
    }

    pub(crate) async fn wait(&self) {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if lock(&self.state).active == 0 {
                return;
            }
            ready.await;
        }
    }
}

fn range(offset: usize, len: usize, size: usize) -> Result<Range<usize>, Error> {
    let end = offset.checked_add(len).ok_or(Error::Bounds)?;
    if end > size {
        return Err(Error::Bounds);
    }
    Ok(offset..end)
}

/// A registered, transferable byte range.
///
/// The engine keeps access while the region is registered. Dropping the handle
/// does not wait for in-flight work; call [`Engine::deregister`] to close the
/// registration and wait until no transfer can still touch the buffer.
#[derive(Clone)]
pub struct MemoryRegion {
    inner: Arc<Region>,
}

impl MemoryRegion {
    pub(crate) fn new(owner: u64, data: Vec<u8>, permissions: Permissions) -> Self {
        Self {
            inner: Arc::new(Region::new(owner, data, permissions)),
        }
    }

    pub fn id(&self) -> RegionId {
        self.inner.id
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>, Error> {
        let _guard = self.inner.acquire(Access::Read)?;
        self.inner.read(offset, len)
    }

    pub fn write(&self, offset: usize, bytes: &[u8]) -> Result<(), Error> {
        let _guard = self.inner.acquire(Access::Write)?;
        self.inner.write(offset, bytes)
    }

    pub(crate) fn inner(&self) -> &Arc<Region> {
        &self.inner
    }

    pub fn remote(&self, endpoint: std::net::SocketAddr) -> RemoteRegion {
        RemoteRegion {
            endpoint,
            id: self.id(),
        }
    }
}

impl From<&MemoryRegion> for MemoryRegion {
    fn from(region: &MemoryRegion) -> Self {
        region.clone()
    }
}
