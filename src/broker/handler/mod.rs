use kafka_protocol::messages::{RequestKind, ResponseKind};
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use kafka_protocol::protocol::{Request};

use crate::broker::config::{BrokerConfig, BrokerId};
use crate::broker::handler::api_versions::ApiVersionsHandler;
use crate::broker::handler::create_topics::CreateTopicsHandler;
use crate::broker::handler::find_coordinator::FindCoordinatorHandler;
use crate::broker::handler::list_groups::ListGroupsHandler;
use crate::broker::handler::metadata::MetadataHandler;
use crate::broker::state::Store;
use crate::error::Result;
use crate::raft::client::RaftClient;

mod api_versions;
mod create_topics;
mod find_coordinator;
mod list_groups;
mod metadata;
mod test;

#[async_trait]
trait Handler<Req, Res = <Req as Request>::Response>: Debug
where
    Req: Request + Default + Debug + Send + 'static,
    Res: Default + Debug + Send,
{
    #[tracing::instrument]
    async fn do_handle(req: Req, ctrl: &Controller) -> Result<Res> {
        tracing::debug!("executing request");
        Self::handle(req, Self::response(), ctrl).await
    }

    async fn handle(req: Req, mut res: Res, ctrl: &Controller) -> Result<Res>;

    fn response() -> Res {
        Res::default()
    }
}

pub struct Controller {
    store: Store,
    client: RaftClient,
    config: BrokerConfig,
}

impl Debug for Controller {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Controller {{}}")
    }
}

impl Controller {
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
                let res = ApiVersionsHandler::do_handle(req, self).await?;
                ResponseKind::ApiVersionsResponse(res)
            }
            RequestKind::MetadataRequest(req) => {
                let res = MetadataHandler::do_handle(req, self).await?;
                ResponseKind::MetadataResponse(res)
            }
            RequestKind::CreateTopicsRequest(req) => {
                let res = CreateTopicsHandler::do_handle(req, self).await?;
                ResponseKind::CreateTopicsResponse(res)
            }
            RequestKind::ListGroupsRequest(req) => {
                let res = ListGroupsHandler::do_handle(req, self).await?;
                ResponseKind::ListGroupsResponse(res)
            }
            RequestKind::FindCoordinatorRequest(req) => {
                let res = FindCoordinatorHandler::do_handle(req, self).await?;
                ResponseKind::FindCoordinatorResponse(res)
            }
            _ => panic!(),
        };

        Ok(res)
    }
}
