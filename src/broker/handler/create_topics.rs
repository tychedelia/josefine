use crate::broker::fsm::Transition;
use crate::broker::state::topic::Topic;
use anyhow::Result;
use async_trait::async_trait;
use std::net::SocketAddr;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, LeaderAndIsrRequest, RequestHeader,
    RequestKind,
};

use crate::broker::config::BrokerId;
use crate::broker::handler::Handler;
use crate::broker::Broker;
use rand::seq::SliceRandom;
use rand::thread_rng;

use uuid::Uuid;

use crate::broker::state::partition::{Partition, PartitionIdx};
use crate::kafka::KafkaClient;
use crate::Shutdown;

impl Broker {
    async fn make_partitions(&self, name: &str, topic: &CreatableTopic) -> Result<Vec<Partition>> {
        let mut brokers = self.get_broker_ids();

        if topic.replication_factor > brokers.len() as i16 {
            // TODO
        }

        let mut partitions = Vec::new();

        for i in 0..topic.num_partitions {
            brokers.shuffle(&mut thread_rng());
            let leader = brokers
                .first()
                .expect("no brokers provided in configuration");

            let replicas: Vec<i32> = brokers
                .iter()
                .take(topic.replication_factor as usize)
                .map(|x| x.0)
                .collect();

            let partition = Partition {
                id: Uuid::new_v4(),
                idx: PartitionIdx(i),
                topic: name.to_string(),
                isr: replicas.clone(),
                assigned_replicas: replicas,
                leader: BrokerId(leader.0),
            };

            partitions.push(partition);
        }

        Ok(partitions)
    }

    async fn create_topic(&self, name: &str, t: CreatableTopic) -> Result<CreatableTopicResult> {
        let topic = Topic {
            id: Uuid::new_v4(),
            name: (*name).to_string(),
            internal: false,
            ..Default::default()
        };

        let mut res = CreatableTopicResult::default();
        res.topic_id = topic.id;
        res.num_partitions = t.num_partitions;
        res.replication_factor = t.replication_factor;

        self.client
            .propose(Transition::EnsureTopic(topic).serialize()?)
            .await?;

        let ps = self.make_partitions(name, &t).await?;

        // TODO we should really do topic + partitions within single tx
        for p in ps {
            let _ = &self
                .client
                .propose(Transition::EnsurePartition(p).serialize()?)
                .await?;
        }

        // Start isr
        for b in self.get_brokers() {
            let mut header = RequestHeader::default();
            header.request_api_version = 5;
            header.request_api_key = ApiKey::LeaderAndIsrKey as i16;
            let mut req = LeaderAndIsrRequest::default();
            req.controller_id = kafka_protocol::messages::BrokerId(b.id.0);
            if b.id == self.config.id {
                self.do_handle(req).await?;
            } else {
                let req = RequestKind::LeaderAndIsrRequest(req);
                let client = KafkaClient::new(SocketAddr::new(b.ip, b.port)).await?;
                let shutdown = Shutdown::new();
                let client = client.connect(shutdown).await?;
                //
                if let kafka_protocol::messages::ResponseKind::LeaderAndIsrResponse(_res) =
                    client.send(header, req).await?
                {
                } else {
                    panic!();
                }
            }
        }

        Ok(res)
    }
}

#[async_trait]
impl Handler<CreateTopicsRequest> for Broker {
    async fn handle(
        &self,
        req: CreateTopicsRequest,
        mut res: CreateTopicsResponse,
    ) -> Result<CreateTopicsResponse> {
        for (name, topic) in req.topics.into_iter() {
            if self.store.topic_exists(&name)? {
                // TODO
            }

            let t = self.create_topic(&name, topic).await?;
            res.topics.insert(name, t);
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::handler::test::new_broker;
    use std::collections::HashMap;

    use crate::broker::handler::Handler;
    use crate::broker::state::topic::Topic;
    use anyhow::Result;
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse, TopicName};
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (mut rx, broker) = new_broker();
        let mut req = CreateTopicsRequest::default();
        let topic_name = TopicName(StrBytes::from_str("Test"));
        req.topics
            .insert(topic_name.clone(), CreatableTopic::default());
        let (res, _) = tokio::join!(
            tokio::spawn(async move { broker.handle(req, CreateTopicsResponse::default()).await }),
            tokio::spawn(async move {
                let (_, cb) = rx.recv().await.unwrap();
                let topic = Topic {
                    id: uuid::Uuid::new_v4(),
                    name: "Test".to_string(),
                    internal: false,
                    partitions: HashMap::new(),
                };
                cb.send(Ok(crate::raft::rpc::Response::new(bincode::serialize(
                    &topic,
                )?)))
                .unwrap();
                Ok::<_, anyhow::Error>(())
            }),
        );

        let res = res??;
        let name = res.topics.keys().next().unwrap();
        assert_eq!(&topic_name, name);
        Ok(())
    }
}
