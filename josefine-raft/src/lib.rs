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

use slog::Logger;

use crate::config::RaftConfig;
use crate::logger::get_instance_logger;
use crate::raft::{RaftActor, setup};

mod log;
mod error;
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
mod logger;

pub struct JosefineBuilder {
    config: RaftConfig,
    logger: &'static Logger,
}

impl JosefineBuilder {
    pub fn new() -> JosefineBuilder {
        let config = RaftConfig::default();
        JosefineBuilder {
            logger: logger::get_root_logger(),
            config,
        }
    }

    pub fn with_log(self, logger: &'static Logger) -> Self {
        JosefineBuilder {
            logger,
            ..self
        }
    }

    pub fn with_config(self, config: RaftConfig) -> Self {
        JosefineBuilder {
            logger: logger::get_root_logger(),
            config,
        }
    }

    pub fn with_config_path(self, config_path: &str) -> Self {
        JosefineBuilder {
            config: RaftConfig::config(config_path),
            ..self
        }
    }

    pub fn build(self) {
        setup::<RaftActor>(get_instance_logger(self.logger, &self.config), self.config, None);
    }
}


