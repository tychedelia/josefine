use crate::broker::broker::{Broker, Handler};

use async_trait::async_trait;


use kafka_protocol::messages::{ProduceRequest};
use kafka_protocol::protocol::{Request};


#[async_trait]
impl Handler<ProduceRequest> for Broker {
    async fn handle(&self, _req: ProduceRequest, res: <ProduceRequest as Request>::Response) -> crate::error::Result<<ProduceRequest as Request>::Response> {

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
