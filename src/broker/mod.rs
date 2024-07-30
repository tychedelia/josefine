use crate::broker::config::BrokerConfig;
use crate::broker::handler::Handler;
use crate::raft::client::RaftClient;
use anyhow::Result;
use derive_more::Display;
use kafka_protocol::messages::{
    ApiVersionsRequest, CreateTopicsRequest, FindCoordinatorRequest, ListGroupsRequest,
    MetadataRequest, RequestKind, ResponseKind,
};
use server::Server;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

use crate::broker::replica::Replica;

use crate::Shutdown;
use state::Store;

pub mod config;
pub mod fsm;
mod handler;
mod log;
mod replica;
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

    pub async fn run(self, client: RaftClient, store: Store, shutdown: Shutdown) -> Result<()> {
        let server = Server::new(self.config);
        server.run(client, store, shutdown).await
    }
}

pub struct Replicas {
    replicas: RwLock<HashMap<Uuid, Arc<Mutex<Replica>>>>,
}

impl Replicas {
    pub fn new() -> Self {
        Self {
            replicas: Default::default(),
        }
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

    fn get_broker_ids(&self) -> Vec<BrokerId> {
        let mut ids: Vec<BrokerId> = self.config.peers.iter().map(|x| x.id).collect();
        ids.push(self.config.id);
        ids
    }

    fn get_brokers(&self) -> Vec<crate::broker::config::Peer> {
        let mut brokers = self.config.peers.clone();
        brokers.push(crate::broker::config::Peer {
            id: self.config.id,
            ip: self.config.ip,
            port: self.config.port,
        });
        brokers
    }

    #[tracing::instrument]
    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        tracing::debug!("handle request");
        let res = match req {
            RequestKind::ApiVersions(req) => {
                let res = self
                    .handle(req, <Broker as Handler<ApiVersionsRequest>>::response())
                    .await?;
                ResponseKind::ApiVersions(res)
            }
            RequestKind::Metadata(req) => {
                let res = self
                    .handle(req, <Broker as Handler<MetadataRequest>>::response())
                    .await?;
                ResponseKind::Metadata(res)
            }
            RequestKind::CreateTopics(req) => {
                let res = self
                    .handle(req, <Broker as Handler<CreateTopicsRequest>>::response())
                    .await?;
                ResponseKind::CreateTopics(res)
            }
            RequestKind::ListGroups(req) => {
                let res = self
                    .handle(req, <Broker as Handler<ListGroupsRequest>>::response())
                    .await?;
                ResponseKind::ListGroups(res)
            }
            RequestKind::FindCoordinator(req) => {
                let res = self
                    .handle(req, <Broker as Handler<FindCoordinatorRequest>>::response())
                    .await?;
                ResponseKind::FindCoordinator(res)
            }
            _ => panic!(),
        };

        Ok(res)
    }
}

#[derive(
    Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Display, Ord, PartialOrd,
)]
pub struct BrokerId(pub i32);
