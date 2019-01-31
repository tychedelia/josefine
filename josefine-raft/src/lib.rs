#![crate_name = "josefine_raft"]
//#![deny(missing_docs)]

//! This implementation of the [Raft](raft.github.io) consensus algorithm forms the basis for safely
//! replicating state in the distributed commit log. Raft is used to elect a leader that coordinates
//! replication across the cluster and ensures that logs are written to a quorom of followers before
//! applying that log to the committed index.
//!
//! This implementation is developed as an agnostic library that could be used for other
//! applications, and does not reference concerns specfic to the distributed log implementation.
//! Raft is itself a commit log and tracks its state in a manner that is somewhat similar to the
//! Kafka reference implementation for Josefine.
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate threadpool;
extern crate failure;
#[macro_use]
extern crate failure_derive;

pub mod io;
mod follower;
mod candidate;
mod leader;
mod election;

/// [Raft](raft.github.io) is a state machine for replicated consensus.
///
/// This implementation focuses on the logic of the state machine and delegates concerns about
/// storage and the RPC protocol to the implementer of the actual Raft server that contains the
/// state machine.
///
/// # Example
///
/// ```
/// #[macro_use]
/// extern crate slog;
/// extern crate slog_async;
/// extern crate slog_term;
///
/// use slog::Drain;
/// use slog::Logger;
/// use josefine_raft::server::RaftServer;
/// use josefine_raft::config::RaftConfig;
/// use josefine_raft::raft::RaftHandle;
/// use josefine_raft::io::MemoryIo;
/// use josefine_raft::rpc::NoopRpc;
/// use std::sync::mpsc;
/// use std::sync::Arc;
/// use std::sync::RwLock;
/// use std::collections::HashMap;
///
/// fn main() {
///     let decorator = slog_term::TermDecorator::new().build();
///     let drain = slog_term::FullFormat::new(decorator).build().fuse();
///     let drain = slog_async::Async::new(drain).build().fuse();
///
///     let logger = Logger::root(drain, o!());
///     let config = RaftConfig::default();
///     let (tx, rx) = mpsc::channel();
///     let raft = RaftHandle::new(config, tx, MemoryIo::new(), NoopRpc::new(), logger, Arc::new(RwLock::new(HashMap::new())));
/// }
/// ```
///
pub mod raft;

/// Raft can be configured with a variety of options.
pub mod config;
mod progress;
pub mod rpc;
mod log;

/// The Raft server contains a raft state machine and handles input to the state machine, providing
/// an RPC implementation to handle communication with other nodes and an IO implementation that
/// handles persisting entries to the commit log.
pub mod server;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
