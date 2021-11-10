

use kafka_protocol::messages::{RequestKind, ResponseKind};

use async_trait::async_trait;

use crate::broker::broker::Broker;
use crate::broker::command::api_versions::ApiVersionsCommand;
use crate::broker::command::create_topics::CreateTopicsCommand;
use crate::broker::command::list_groups::ListGroupsCommand;
use crate::broker::command::metadata::MetadataCommand;
use crate::broker::config::BrokerConfig;
use crate::error::Result;
use crate::raft::client::RaftClient;

mod create_topics;
mod metadata;
mod api_versions;
mod list_groups;

#[async_trait]
trait Command {
    type Request : Default;
    type Response : Default;

    async fn execute(req: Self::Request, ctrl: &Controller) -> Result<Self::Response>;

    fn response() -> Self::Response {
        Self::Response::default()
    }
}

pub struct Controller {
    broker: Broker,
    client: RaftClient,
    config: BrokerConfig,
}

impl Controller {
    pub fn new(broker: Broker, client: RaftClient, config: BrokerConfig) -> Self {
        Self {
            broker, client, config
        }
    }

    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        let res = match req {
            RequestKind::ApiVersionsRequest(req) => {
                let res = ApiVersionsCommand::execute(req, self).await?;
                ResponseKind::ApiVersionsResponse(res)
            }
            RequestKind::MetadataRequest(req) => {
                let res = MetadataCommand::execute(req, self).await?;
                ResponseKind::MetadataResponse(res)
            }
            RequestKind::CreateTopicsRequest(req) => {
                let res = CreateTopicsCommand::execute(req, self).await?;
                ResponseKind::CreateTopicsResponse(res)
            }
            RequestKind::ListGroupsRequest(req) => {
                let res = ListGroupsCommand::execute(req, self).await?;
                ResponseKind::ListGroupsResponse(res)
            }
            _ => panic!()
        };

        Ok(res)
    }
}