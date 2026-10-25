use std::io;

/// Backend identity is explicit so callers never infer one-sided semantics
/// from an interface that is only a request/response simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Tcp,
    Rdma,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid config: {0}")]
    Config(&'static str),
    #[error("region is not registered on this engine")]
    Region,
    #[error("region is closed")]
    Closed,
    #[error("permission denied")]
    Permission,
    #[error("range out of bounds")]
    Bounds,
    #[error("transfer exceeds the configured frame limit")]
    Size,
    #[error("authentication failed")]
    Auth,
    #[error("connection closed")]
    Disconnected,
    #[error("remote region is quarantined; its owner must deregister it before reuse")]
    Quarantined,
    #[error("remote write outcome is unknown for {remote}: {source}")]
    Uncertain {
        remote: crate::region::RemoteRegion,
        #[source]
        source: Box<Error>,
    },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("remote error: {0}")]
    Remote(String),
    #[error("tcp read failed: {0}")]
    TcpRead(#[source] io::Error),
    #[error("tcp write failed: {0}")]
    TcpWrite(#[source] io::Error),
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

impl Error {
    pub(crate) fn tcp(self, read: bool) -> Self {
        match (self, read) {
            (Error::Io(error), true) => Error::TcpRead(error),
            (Error::Io(error), false) => Error::TcpWrite(error),
            (other, _) => other,
        }
    }
}
