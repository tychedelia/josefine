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

use crate::raft::RaftHandle;

use josefine_core::error::Result;
use std::time::Duration;
use rpc::{Proposal, Response};
use tokio::sync::oneshot;
use tokio::sync::mpsc::UnboundedReceiver;

mod candidate;
mod election;
pub mod error;
mod follower;
mod leader;
mod log;
pub mod rpc;
mod store;

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
pub mod fsm;
mod test;
pub mod client;

pub struct JosefineRaft {
    server: server::Server,
}

impl JosefineRaft {
    pub fn new(config: config::RaftConfig) -> Self {
        JosefineRaft {
            server: server::Server::new(config),
        }
    }

    pub fn with_config<P: AsRef<std::path::Path>>(path: P) -> Self {
        let config = config::RaftConfig::config(path.as_ref());
        Self::new(config)
    }

    pub async fn run<T: 'static + fsm::Fsm>(self, fsm: T, client_rx: UnboundedReceiver<(Proposal, oneshot::Sender<Result<Response>>)>) -> Result<RaftHandle> {
        self.server.run(None, fsm, client_rx).await
    }

    pub async fn run_for<T: 'static + fsm::Fsm>(self, duration: Duration, fsm: T, client_rx: UnboundedReceiver<(Proposal, oneshot::Sender<Result<Response>>)>) -> Result<RaftHandle> {
        self.server.run(Some(duration), fsm, client_rx).await
    }
}
