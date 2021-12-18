use crate::broker::handler::{Controller, Handler};
use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, ProduceRequest, TopicName};
use kafka_protocol::protocol::{Request, StrBytes};
use string::TryFrom;

#[derive(Debug)]
pub struct ProduceHandler;

#[async_trait]
impl Handler<ProduceRequest> for ProduceHandler {
    async fn handle(req: ProduceRequest, res: <ProduceRequest as Request>::Response, ctrl: &Controller) -> crate::error::Result<<ProduceRequest as Request>::Response> {


        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::ProduceResponse;
    use super::*;
    use crate::error::Result;
    use crate::broker::handler::test::new_controller;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, ctrl) = new_controller();
        let req = ProduceRequest::default();
        let _res = ProduceHandler::handle(req, ProduceResponse::response(), &ctrl).await?;
        Ok(())
    }
}
