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
use crate::io::{MemoryIo, Io};
use crate::raft::{ApplyStep, RaftContainer, RaftRole};
use crate::rpc::{TpcRpc, Rpc};
use slog::Logger;

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


pub struct JosefineBuilder {
    config: RaftConfig,
    log: Logger,
    role_change_cb: Option<Box<FnOnce(RaftRole)>>,
}


impl JosefineBuilder {
    pub fn new() -> JosefineBuilder {
        JosefineBuilder {
            config: RaftConfig::default(),
            role_change_cb: None,
            log: log::get_root_logger(),
        }
    }

    pub fn with_config(self, config: RaftConfig) -> Self {
        JosefineBuilder {
            config,
            ..self
        }
    }

    pub fn role_change_cb<CB: 'static + FnOnce(RaftRole)>(self, cb: CB) -> Self {
        JosefineBuilder {
            role_change_cb: Some(Box::new(cb)),
            ..self
        }
    }

    pub fn build(self) -> Josefine {
        let (tx, rx) = channel::<ApplyStep>();
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let io = MemoryIo::new();
        let rpc = TpcRpc::new(self.config.clone(), tx.clone(), nodes.clone(), self.log.new(o!()));

        RaftContainer::new(self.config.clone(), tx.clone(), io, rpc, self.log, nodes.clone())
    }
}


