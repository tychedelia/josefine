use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::raft::client::RaftClient;
use crate::raft::rpc::Proposal;
use crate::raft::rpc::Response;
use server::Server;
use sled::Db;
use crate::broker::broker::Broker;
use crate::broker::config::BrokerConfig;
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
