

use kafka_protocol::messages::{RequestKind, ResponseKind};

use async_trait::async_trait;

use crate::broker::broker::Broker;
use crate::broker::command::api_versions::ApiVersionsCommand;
use crate::broker::command::create_topics::CreateTopicsCommand;
use crate::broker::command::metadata::MetadataCommand;
use crate::error::Result;
use crate::raft::client::RaftClient;

pub mod create_topics;
pub mod metadata;
pub mod api_versions;

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
}

impl Controller {
    pub fn new(broker: Broker, client: RaftClient) -> Self {
        Self {
            broker, client
        }
    }

    pub async fn handle_request(&self, req: RequestKind) -> Result<ResponseKind> {
        let res = match req {
            RequestKind::ApiVersionsRequest(req) => {
                let res = ApiVersionsCommand::execute(req, &self).await?;
                ResponseKind::ApiVersionsResponse(res)
            }
            RequestKind::MetadataRequest(req) => {
                let res = MetadataCommand::execute(req, &self).await?;
                ResponseKind::MetadataResponse(res)
            }
            RequestKind::CreateTopicsRequest(req) => {
                let res = CreateTopicsCommand::execute(req, &self).await?;
                ResponseKind::CreateTopicsResponse(res)
            }
            _ => panic!()
        };

        Ok(res)
    }
}