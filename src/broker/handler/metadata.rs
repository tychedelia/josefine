use crate::broker::handler::{Controller, Handler};
use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use string::TryFrom;

#[derive(Debug)]
pub struct MetadataHandler;

#[async_trait]
impl Handler<MetadataRequest> for MetadataHandler {
    async fn handle(
        _: MetadataRequest,
        mut res: MetadataResponse,
        ctrl: &Controller,
    ) -> crate::error::Result<MetadataResponse> {
        res.brokers.insert(
            BrokerId(ctrl.config.id.0),
            MetadataResponseBroker {
                host: ctrl.config.ip.to_string().to_str_bytes(),
                port: ctrl.config.port as i32,
                rack: None,
                unknown_tagged_fields: Default::default(),
            },
        );
        res.controller_id = BrokerId(1);
        res.cluster_id = Some(StrBytes::from_str("josefine"));

        let topics = ctrl.store.get_topics()?;
        for (name, topic) in topics.into_iter() {
            let t = MetadataResponseTopic {
                topic_id: topic.id,
                partitions: topic.partitions.iter()
                    .map(|(k, v)| {
                        let mut mp = MetadataResponsePartition::default();
                        match ctrl.store.get_partition(&topic.name, *k).unwrap() {
                            Some(p) => {
                                // mp.leader_id messages:: = p.leader;
                                mp.partition_index = p.idx.0;
                                mp.isr_nodes = p.isr.into_iter().map(BrokerId).collect();
                                mp.replica_nodes = p.assigned_replicas.into_iter().map(BrokerId).collect();
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
    use kafka_protocol::messages::MetadataRequest;

    use crate::broker::handler::metadata::MetadataHandler;
    use crate::broker::handler::test::new_controller;
    use crate::broker::handler::Handler;

    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, ctrl) = new_controller();
        let req = MetadataRequest::default();
        let _res = MetadataHandler::handle(req, MetadataHandler::response(), &ctrl).await?;
        Ok(())
    }
}
