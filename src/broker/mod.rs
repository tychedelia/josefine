use crate::error::Result;
use crate::raft::client::RaftClient;

use crate::broker::config::BrokerConfig;
use server::Server;

use crate::broker::store::Store;

pub(crate) mod store;
mod command;
pub mod config;
mod entry;
pub mod fsm;
mod index;
mod log;
mod state;
mod partition;
mod segment;
mod server;
mod tcp;

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
        store: Store,
        shutdown: (
            tokio::sync::broadcast::Sender<()>,
            tokio::sync::broadcast::Receiver<()>,
        ),
    ) -> Result<()> {
        let server = Server::new(self.config);
        server.run(client, store, shutdown).await
    }
}
