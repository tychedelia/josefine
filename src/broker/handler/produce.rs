use async_trait::async_trait;
use std::io::Write;

use crate::broker::handler::Handler;
use crate::broker::Broker;

use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::protocol::Request;

#[async_trait]
impl Handler<ProduceRequest> for Broker {
    async fn handle(
        &self,
        req: ProduceRequest,
        res: <ProduceRequest as Request>::Response,
    ) -> anyhow::Result<<ProduceRequest as Request>::Response> {
        for (t, td) in req.topic_data.iter() {
            let _topic = self.store.get_topic(t)?.expect("TODO: topic doesn't exist");
            for pd in td.partition_data.iter() {
                if let Some(bytes) = &pd.records {
                    let p = self
                        .store
                        .get_partition(t, pd.index)?
                        .expect("TODO: partition doesn't exist");
                    let replica = self
                        .replicas
                        .get(p.id)
                        .expect("TODO: replica doesn't exist");
                    let mut replica = replica.lock().expect("mutex poisoned");
                    replica.log.write(&bytes[..])?;
                }
            }
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::handler::test::new_broker;
    use anyhow::Result;
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
