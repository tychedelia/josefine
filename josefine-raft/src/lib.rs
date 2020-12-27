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
#[macro_use]
extern crate lazy_static;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use crate::config::RaftConfig;
use crate::raft::RaftHandle;

use futures_util::core_reexport::time::Duration;
use crate::fsm::StateMachine;

mod candidate;
mod election;
pub mod error;
mod follower;
mod leader;
mod log;
mod rpc;

/// [Raft](raft.github.io) is a state machine for replicated consensus.
///
/// This implementation focuses on the logic of the state machine and delegates concerns about
/// storage and the RPC protocol to the implementer of the actual Raft server that contains the
/// state machine.
pub mod raft;

/// Raft can be configured with a variety of options.
pub mod config;
mod logger;
mod progress;
mod server;
mod tcp;
mod fsm;
mod store;

pub struct JosefineRaft {
    server: server::Server,
}

impl JosefineRaft {
    pub fn new() -> Self {
        let config = config::RaftConfig::config("./config.toml");
        JosefineRaft {
            server: server::Server::new(config),
        }
    }

    pub fn with_config(config: RaftConfig) -> Self {
        JosefineRaft {
            server: server::Server::new(config),
        }
    }

    pub async fn run(self) -> error::Result<RaftHandle> {
        self.server.run(Box::new(crate::fsm::TestState { last_index: 0 }), None).await
    }

    pub async fn run_for(self, duration: Duration) -> error::Result<RaftHandle> {
        self.server.run(Box::new(crate::fsm::TestState { last_index: 0 }), Some(duration)).await
    }
}
