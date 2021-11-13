use std::fmt::{Debug, Formatter};
use kafka_protocol::messages::{RequestKind, ResponseKind};

use async_trait::async_trait;

use crate::broker::store::Store;
use crate::broker::command::api_versions::ApiVersionsCommand;
use crate::broker::command::create_topics::CreateTopicsCommand;
use crate::broker::command::find_coordinator::FindCoordinatorCommand;
use crate::broker::command::list_groups::ListGroupsCommand;
use crate::broker::command::metadata::MetadataCommand;
use crate::broker::config::BrokerConfig;
use crate::error::Result;
use crate::raft::client::RaftClient;

mod api_versions;
mod create_topics;
mod list_groups;
mod metadata;
mod find_coordinator;
mod test;

#[async_trait]
trait Command {
    type Request: Default + Debug + Send;
    type Response: Default + Debug + Send;

    #[tracing::instrument]
    async fn doExecute(req: Self::Request, ctrl: &Controller) -> Result<Self::Response> {
        tracing::debug!("executing request");
        Self::execute(req, ctrl).await
    }

    async fn execute(req: Self::Request, ctrl: &Controller) -> Result<Self::Response>;

    fn response() -> Self::Response {
        Self::Response::default()
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

    #[tracing::instrument]
    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        tracing::info!("handle_request");
        let res = match req {
            RequestKind::ApiVersionsRequest(req) => {
                let res = ApiVersionsCommand::doExecute(req, self).await?;
                ResponseKind::ApiVersionsResponse(res)
            }
            RequestKind::MetadataRequest(req) => {
                let res = MetadataCommand::doExecute(req, self).await?;
                ResponseKind::MetadataResponse(res)
            }
            RequestKind::CreateTopicsRequest(req) => {
                let res = CreateTopicsCommand::doExecute(req, self).await?;
                ResponseKind::CreateTopicsResponse(res)
            }
            RequestKind::ListGroupsRequest(req) => {
                let res = ListGroupsCommand::doExecute(req, self).await?;
                ResponseKind::ListGroupsResponse(res)
            }
            RequestKind::FindCoordinatorRequest(req) => {
                let res = FindCoordinatorCommand::doExecute(req, self).await?;
                ResponseKind::FindCoordinatorResponse(res)
            }
            _ => panic!(),
        };

        Ok(res)
    }
}