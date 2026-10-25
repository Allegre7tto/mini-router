//! Asynchronous registered-memory transfer.
//!
//! The TCP backend simulates remote `read` and `write` with request/response
//! messages; it does not provide one-sided RDMA semantics. `Kind` and the
//! `TcpRead`/`TcpWrite` errors make that distinction explicit.
//!
//! A transfer is submitted with [`Engine::read`], [`Engine::write`], or
//! [`Engine::submit`] and returns a [`Handle`]. The handle yields exactly one
//! terminal [`Completion`]; duplicate network notifications are discarded.
//!
//! Cancellation and deadlines stop local publication, but completion waits for
//! submitted work to drain. A write may already have committed. If its reply is
//! lost, [`Error::Uncertain`] requires the remote owner to deregister the target
//! before reuse; a disconnect is not proof that the write did not happen.
//! Remote descriptors round-trip through `Display` and `FromStr`.

#![forbid(unsafe_code)]

mod engine;
mod error;
mod protocol;
mod region;
mod server;
mod sync;
mod transport;

pub use engine::{Completion, Config, Direction, Engine, Handle, Transfer};
pub use error::{Error, Kind};
pub use region::{Access, MemoryRegion, Permissions, RegionId, RemoteRegion};
pub use transport::Transport;
