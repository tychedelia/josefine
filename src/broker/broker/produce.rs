use crate::broker::broker::{Broker, Handler};
use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, ProduceRequest, TopicName};
use kafka_protocol::protocol::{Request, StrBytes};
use string::TryFrom;

#[async_trait]
impl Handler<ProduceRequest> for Broker {
    async fn handle(&self, req: ProduceRequest, res: <ProduceRequest as Request>::Response) -> crate::error::Result<<ProduceRequest as Request>::Response> {

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::ProduceResponse;
    use super::*;
    use crate::error::Result;
    use crate::broker::broker::test::new_broker;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let _res = broker.handle(ProduceRequest::default(), ProduceResponse::default()).await?;
        Ok(())
    }
}
