use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::ResponseError::{TopicAlreadyExists, UnknownTopicOrPartition};

use crate::broker::handler::Handler;
use crate::broker::state::topic::Topic;
use crate::broker::Broker;

impl Handler<MetadataRequest> for Broker {
    #[tracing::instrument]
    async fn handle(
        &self,
        req: MetadataRequest,
        mut res: MetadataResponse,
    ) -> anyhow::Result<MetadataResponse> {
        self.get_brokers().iter().for_each(|b| {
            res.brokers.insert(
                BrokerId(b.id.0),
                MetadataResponseBroker::default()
                    .with_host(b.ip.to_string().into())
                    .with_port(b.port as i32),
            );
        });

        res.controller_id = BrokerId(1);
        res.cluster_id = Some("josefine".into());
        res.throttle_time_ms = 1000;

        if let Some(topics) = req.topics {
            self.get_topic_metadata(&mut res, topics)?;
        } else {
            self.get_all_topic_metadata(&mut res)?;
        }

        Ok(res)
    }
}

impl Broker {
    fn get_topic_metadata(
        &self,
        res: &mut MetadataResponse,
        topics: Vec<MetadataRequestTopic>,
    ) -> anyhow::Result<()> {
        for topic_req in topics.into_iter() {
            let name = topic_req.name.unwrap();
            let topic = self.store.get_topic(&name)?;

            if let Some(topic) = topic {
                let t = self.build_topic_metadata(name.to_string(), &topic)?;
                res.topics.insert(name, t);
            } else {
                res.topics.insert(
                    name,
                    MetadataResponseTopic::default()
                        .with_error_code(UnknownTopicOrPartition.code()),
                );
            }
        }
        Ok(())
    }

    fn get_all_topic_metadata(&self, res: &mut MetadataResponse) -> anyhow::Result<()> {
        let topics = self.store.get_topics()?;
        for (name, topic) in topics.into_iter() {
            let t = self.build_topic_metadata(name, &topic)?;
            res.topics.insert(TopicName(topic.name.into()), t);
        }
        Ok(())
    }

    fn build_topic_metadata(
        &self,
        name: String,
        topic: &Topic,
    ) -> anyhow::Result<MetadataResponseTopic> {
        let t = MetadataResponseTopic::default()
            .with_topic_id(topic.id)
            .with_partitions(
                topic
                    .partitions
                    .iter()
                    .map(|(k, _v)| {
                        let mut mp = MetadataResponsePartition::default();
                        match self.store.get_partition(&topic.name, *k)? {
                            Some(p) => {
                                // mp.leader_id messages:: = p.leader;
                                mp.leader_id = p.leader.0.into();
                                mp.partition_index = p.idx.0;
                                mp.isr_nodes = p.isr.into_iter().map(BrokerId).collect();
                                mp.replica_nodes =
                                    p.assigned_replicas.into_iter().map(BrokerId).collect();
                                mp.leader_epoch = 3;
                            }
                            None => {
                                tracing::error!("could not fine partition");
                                mp.error_code = 0;
                            }
                        }
                        Ok(mp)
                    })
                    .collect::<anyhow::Result<Vec<MetadataResponsePartition>>>()?,
            );
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use kafka_protocol::messages::{MetadataRequest, MetadataResponse};

    use crate::broker::handler::test::new_broker;
    use crate::broker::handler::Handler;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let _res = broker
            .handle(MetadataRequest::default(), MetadataResponse::default())
            .await?;
        Ok(())
    }
}
