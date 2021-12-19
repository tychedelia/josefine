use async_trait::async_trait;

use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::protocol::Request;
use crate::broker::Broker;
use crate::broker::handler::Handler;
use crate::error::JosefineError;

#[async_trait]
impl Handler<ProduceRequest> for Broker {
    async fn handle(
        &self,
        req: ProduceRequest,
        res: <ProduceRequest as Request>::Response,
    ) -> crate::error::Result<<ProduceRequest as Request>::Response> {
        for (t, td) in req.topic_data.iter() {
            let topic = self.store.get_topic(&t)?.ok_or(JosefineError::Internal { error_msg: format!("unknown topic {:?}", t) });
            

        }


        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::handler::test::new_broker;
    use crate::error::Result;
    use kafka_protocol::messages::ProduceResponse;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let _res = broker
            .handle(ProduceRequest::default(), ProduceResponse::default())
            .await?;
        Ok(())
    }
}
