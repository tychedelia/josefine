use std::fmt::{Debug, Formatter};
use kafka_protocol::messages::{RequestKind, ResponseKind};
use crate::error::Result;
use crate::raft::client::RaftClient;
use crate::broker::handler::Handler;
use crate::broker::config::{BrokerConfig, BrokerId};
use server::Server;

use state::Store;

mod handler;
pub mod config;
mod entry;
pub mod fsm;
mod index;
mod log;
mod segment;
mod server;
pub(crate) mod state;
mod tcp;

pub struct JosefineBroker {
    config: BrokerConfig,
}

impl JosefineBroker {
    pub fn new(config: BrokerConfig) -> Self {
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

pub struct Broker {
    store: Store,
    client: RaftClient,
    config: BrokerConfig,
}

impl Debug for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Controller {{}}")
    }
}

impl Broker {
    pub fn new(store: Store, client: RaftClient, config: BrokerConfig) -> Self {
        Self {
            store,
            client,
            config,
        }
    }

    fn get_brokers(&self) -> Vec<BrokerId> {
        let mut ids: Vec<BrokerId> = self.config.peers.iter().map(|x| x.id).collect();
        ids.push(self.config.id);
        ids
    }

    #[tracing::instrument]
    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        tracing::info!("handle_request");
        let res = match req {
            RequestKind::ApiVersionsRequest(req) => {
                let res = self.do_handle(req).await?;
                ResponseKind::ApiVersionsResponse(res)
            }
            RequestKind::MetadataRequest(req) => {
                let res = self.do_handle(req).await?;
                ResponseKind::MetadataResponse(res)
            }
            RequestKind::CreateTopicsRequest(req) => {
                let res = self.do_handle(req).await?;
                ResponseKind::CreateTopicsResponse(res)
            }
            RequestKind::ListGroupsRequest(req) => {
                let res = self.do_handle(req).await?;
                ResponseKind::ListGroupsResponse(res)
            }
            RequestKind::FindCoordinatorRequest(req) => {
                let res = self.do_handle(req).await?;
                ResponseKind::FindCoordinatorResponse(res)
            }
            _ => panic!(),
        };

        Ok(res)
    }
}
