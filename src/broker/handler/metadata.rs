use crate::broker::handler::Handler;
use crate::broker::Broker;
use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use string::TryFrom;

#[async_trait]
impl Handler<MetadataRequest> for Broker {
    async fn handle(
        &self,
        _: MetadataRequest,
        mut res: MetadataResponse,
    ) -> crate::error::Result<MetadataResponse> {
        res.brokers.insert(
            BrokerId(self.config.id.0),
            MetadataResponseBroker {
                host: self.config.ip.to_string().to_str_bytes(),
                port: self.config.port as i32,
                ..Default::default()
            },
        );
        res.controller_id = BrokerId(1);
        res.cluster_id = Some(StrBytes::from_str("josefine"));

        let topics = self.store.get_topics()?;
        for (name, topic) in topics.into_iter() {
            let t = MetadataResponseTopic {
                topic_id: topic.id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|(k, _v)| {
                        let mut mp = MetadataResponsePartition::default();
                        match self.store.get_partition(&topic.name, *k).unwrap() {
                            Some(p) => {
                                // mp.leader_id messages:: = p.leader;
                                mp.partition_index = p.idx.0;
                                mp.isr_nodes = p.isr.into_iter().map(BrokerId).collect();
                                mp.replica_nodes =
                                    p.assigned_replicas.into_iter().map(BrokerId).collect();
                            }
                            None => {
                                mp.error_code = 0;
                            }
                        }
                        mp
                    })
                    .collect(),
                ..Default::default()
            };
            let s = StrBytes::try_from(Bytes::from(name)).unwrap();
            res.topics.insert(TopicName(s), t);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{MetadataRequest, MetadataResponse};

    use crate::broker::handler::test::new_broker;
    use crate::broker::handler::Handler;

    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let _res = broker
            .handle(MetadataRequest::default(), MetadataResponse::default())
            .await?;
        Ok(())
    }
}
