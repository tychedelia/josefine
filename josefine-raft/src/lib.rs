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





use slog::Logger;

use crate::config::RaftConfig;
use crate::raft::{setup};

mod listener;
mod rpc;
mod node;
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
mod log;

pub struct JosefineBuilder {
    config: RaftConfig,
    log: Logger,
}

impl JosefineBuilder {
    pub fn new() -> JosefineBuilder {
        let config = RaftConfig::default();
        JosefineBuilder {
            log: log::get_root_logger(config.clone()),
            config,
        }
    }

    pub fn with_config(self, config: RaftConfig) -> Self {
        JosefineBuilder {
            config,
            ..self
        }
    }

    pub fn build(self) {
        setup(self.log, self.config);
    }
}


