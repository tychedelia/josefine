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
use actix::{Actor, Arbiter, Context, Handler, Message, Recipient, StreamHandler, Supervised, System, Supervisor, SystemRunner};
use actix::ActorContext;
use actix::AsyncContext;

use slog::Logger;

use crate::config::RaftConfig;
use crate::logger::get_instance_logger;
use crate::raft::{RaftActor, setup};
use std::net::SocketAddr;
use crate::listener::TcpListenerActor;
use std::collections::HashMap;
use crate::node::NodeActor;
use std::thread;
use std::time::Duration;
use crate::rpc::RpcMessage;

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

pub struct JosefineBuilder<T: Actor<Context=Context<T>> + Send> {
    config: RaftConfig,
    actor: Option<T>,
    logger: &'static Logger,
}

impl <T: Actor<Context=Context<T>> + Send> JosefineBuilder<T> {
    pub fn new() -> JosefineBuilder<T> {
        let config = RaftConfig::default();
        JosefineBuilder {
            logger: logger::get_root_logger(),
            actor: None,
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
            ..self
        }
    }

    pub fn with_actor(self, actor: T) -> Self{
        JosefineBuilder {
            actor: Some(actor),
            ..self
        }
    }

    pub fn with_config_path(self, config_path: &str) -> Self {
        JosefineBuilder {
            config: RaftConfig::config(config_path),
            ..self
        }
    }



    pub fn build(self) -> JosefineRaft {
        let system = System::new("raft");

        if let Some(actor) = self.actor {
            Arbiter::start(move |_| actor);
        }

        let logger = self.logger.new(o!());
        let port = self.config.port;
        let config = self.config.clone();
        let _raft = RaftActor::create(move |ctx| {
            let l = logger.new(o!());
            let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), port); // TODO: :(
            let raft = ctx.address().recipient();
            let _server = Supervisor::start(move |_| TcpListenerActor::new(addr, l, raft));

            let mut nodes = HashMap::new();
            for node in &config.nodes {
                let addr = node.addr.clone();
                let l = logger.new(o!());
                let raft = ctx.address().recipient();
                let n = Supervisor::start(move |_| NodeActor::new(addr, l, raft));
                nodes.insert(node.id, n.recipient());
            }

            let raft = ctx.address().recipient();
            thread::spawn(move || {
                loop {
                    thread::sleep(Duration::from_millis(100));
                    raft.try_send(RpcMessage::Tick).unwrap();
                }
            });

            System::current().registry().set(ctx.address());
            RaftActor::new(config, logger, nodes)
        });

        JosefineRaft {
            system
        }
    }
}


pub struct JosefineRaft {
    system: SystemRunner
}

impl JosefineRaft {
    pub fn run(self) -> i32{
        self.system.run()
    }
}
