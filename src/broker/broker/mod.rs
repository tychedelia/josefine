use kafka_protocol::messages::{RequestKind, ResponseKind};
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use kafka_protocol::protocol::Request;

use crate::broker::config::{BrokerConfig, BrokerId};
use crate::broker::state::Store;
use crate::error::Result;
use crate::raft::client::RaftClient;

mod api_versions;
mod create_topics;
mod find_coordinator;
mod list_groups;
mod metadata;
mod produce;
mod test;

#[async_trait]
trait Handler<Req, Res = <Req as Request>::Response>: Debug
where
    Req: Request + Default + Debug + Send + 'static,
    Res: Default + Debug + Send,
{
    #[tracing::instrument]
    async fn do_handle(&self, req: Req) -> Result<Res> {
        tracing::debug!("executing request");
        self.handle(req, Self::response()).await
    }

    async fn handle(&self, req: Req, mut res: Res) -> Result<Res>;

    fn response() -> Res {
        Res::default()
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
