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
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate threadpool;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;

use crate::config::RaftConfig;
use crate::io::MemoryIo;
use crate::raft::{ApplyStep, RaftContainer};
use crate::rpc::TpcRpc;

mod io;
mod follower;
mod candidate;
mod leader;
mod election;

/// [Raft](raft.github.io) is a state machine for replicated consensus.
///
/// This implementation focuses on the logic of the state machine and delegates concerns about
/// storage and the RPC protocol to the implementer of the actual Raft server that contains the
/// state machine.
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

pub type Josefine = RaftContainer<MemoryIo, TpcRpc>;

impl Josefine {
    pub fn with_config(config: RaftConfig) -> Josefine {
        let log = log::get_root_logger();
        let (tx, rx) = channel::<ApplyStep>();

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        let io = MemoryIo::new();
        let rpc = TpcRpc::new(config.clone(), tx.clone(), nodes.clone(), log.new(o!()));
        RaftContainer::new(config.clone(), tx.clone(), io, rpc, log, nodes.clone())
    }
}
