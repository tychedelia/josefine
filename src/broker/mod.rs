use crate::broker::config::{BrokerConfig, BrokerId};
use crate::broker::handler::Handler;
use crate::raft::client::RaftClient;
use anyhow::Result;
use kafka_protocol::messages::{RequestKind, ResponseKind};
use server::Server;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

use crate::broker::replica::Replica;

use state::Store;
use crate::Shutdown;

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

    pub async fn run(
        self,
        client: RaftClient,
        store: Store,
        shutdown: Shutdown,
    ) -> Result<()> {
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

    fn get_brokers(&self) -> Vec<crate::broker::config::Broker> {
        let mut brokers = self.config.peers.clone();
        brokers.push(crate::broker::config::Broker {
            id: self.config.id,
            ip: self.config.ip,
            port: self.config.port,
        });
        brokers
    }

    #[tracing::instrument]
    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        tracing::info!("handle request");
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
