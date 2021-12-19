use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use kafka_protocol::messages::{RequestKind, ResponseKind};
use uuid::Uuid;
use crate::error::Result;
use crate::raft::client::RaftClient;
use crate::broker::handler::Handler;
use crate::broker::config::{BrokerConfig, BrokerId};
use server::Server;

use state::Store;
use crate::broker::log::Log;
use crate::broker::replica::Replica;
use crate::broker::state::partition::PartitionIdx;

mod handler;
pub mod config;
pub mod fsm;
mod log;
pub(crate) mod state;
mod tcp;
mod replica;
mod server;

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

pub struct Replicas {
    replicas: RwLock<HashMap<Uuid, Arc<Mutex<Replica>>>>
}

impl Replicas {
    pub fn new() -> Self {
        Self { replicas: Default::default() }
    }

    pub fn add(&self, id: Uuid, replica: Replica) {
        let mut rs = self.replicas.write().unwrap();
        rs.insert(id, Arc::new(Mutex::new(replica)));
    }

    pub fn get(&self, id: Uuid) -> Option<Arc<Mutex<Replica>>> {
        let rs = self.replicas.read().unwrap();
        rs.get(&id).map(Clone::clone)
    }
}

pub struct Broker {
    store: Store,
    client: RaftClient,
    config: BrokerConfig,
    replicas: Replicas,
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
            replicas: Replicas::new(),
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
