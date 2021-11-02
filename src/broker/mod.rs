use crate::error::Result;
use crate::raft::client::RaftClient;

use crate::broker::config::BrokerConfig;
use server::Server;

use std::net::SocketAddr;
use crate::broker::broker::Broker;

pub(crate) mod broker;
pub mod config;
mod entry;
pub mod fsm;
mod index;
mod log;
mod partition;
mod segment;
mod server;
mod tcp;
mod topic;
mod command;

pub struct JosefineBroker {
    config: BrokerConfig,
}

impl JosefineBroker {
    pub fn with_config(config: BrokerConfig) -> Self {
        JosefineBroker { config }
    }

    pub async fn run(
        self,
        client: RaftClient,
        broker: Broker,
        shutdown: (tokio::sync::broadcast::Sender<()>, tokio::sync::broadcast::Receiver<()>),
    ) -> Result<()> {
        let socket_addr = SocketAddr::new(self.config.ip, self.config.port);
        let server = Server::new(socket_addr);
        server.run(client, broker, shutdown).await
    }
}
