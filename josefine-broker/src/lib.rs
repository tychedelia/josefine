#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use josefine_core::error::Result;
use josefine_raft::client::RaftClient;
use josefine_raft::rpc::Proposal;
use josefine_raft::rpc::Response;
use server::Server;
use sled::Db;
use crate::broker::Broker;
use crate::config::BrokerConfig;
use std::net::SocketAddr;

mod entry;
mod index;
mod log;
mod partition;
mod segment;
mod server;
pub mod fsm;
mod tcp;
mod topic;
mod broker;
pub mod config;

pub struct JosefineBroker {
    config: BrokerConfig
}

impl JosefineBroker {
    pub fn with_config(config: BrokerConfig) -> Self {
        JosefineBroker {
            config
        }
    }

    pub async fn run(self, client: RaftClient, db: &'static Db) -> Result<()> {
        let socket_addr = SocketAddr::new(self.config.ip, self.config.port);
        let server = Server::new(socket_addr);
        server.run(client, Broker::new(db)).await
    }
}
