use std::ops::Index;
use crate::broker::fsm::Transition;
use crate::error::Result;
use crate::broker::handler::{Controller, Handler};
use crate::broker::state::topic::Topic;
use async_trait::async_trait;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::leader_and_isr_response::LeaderAndIsrTopicError;
use rand::seq::SliceRandom;
use rand::thread_rng;
use uuid::Uuid;
use crate::broker::config::BrokerId;
use crate::broker::state::partition::{Partition, PartitionIdx};

#[derive(Debug)]
pub struct CreateTopicsHandler;

impl CreateTopicsHandler {
    async fn make_partitions(name: &str, topic: &CreatableTopic, ctrl: &Controller) -> Result<Vec<Partition>> {
        let mut brokers = ctrl.get_brokers();

        if topic.replication_factor > brokers.len() as i16 {
            // TODO
        }

        let mut partitions = Vec::new();

        for i in 0..topic.num_partitions {
            brokers.shuffle(&mut thread_rng());
            let leader = brokers.first().unwrap();

            let replicas: Vec<i32> = brokers.iter()
                .take(topic.replication_factor as usize)
                .map(|x| x.0)
                .collect();

            let partition = Partition {
                idx: PartitionIdx(i),
                topic: name.to_string(),
                isr: replicas.clone(),
                assigned_replicas: replicas,
                leader: leader.0,
            };

            partitions.push(partition);
        }

        Ok(partitions)
    }

    async fn create_topic(name: &str, t: CreatableTopic, ctrl: &Controller) -> Result<CreatableTopicResult> {
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

        &ctrl
            .client
            .propose(Transition::EnsureTopic(topic).serialize()?)
            .await?;

        let ps = Self::make_partitions(name, &t, ctrl).await?;

        // TODO we should really do topic + partitions within single tx
        for p in ps {
            &ctrl.client.propose(Transition::EnsurePartition(p).serialize()?).await?;
        }

        Ok(res)
    }
}

#[async_trait]
impl Handler<CreateTopicsRequest> for CreateTopicsHandler {
    async fn handle(
        req: CreateTopicsRequest,
        mut res: CreateTopicsResponse,
        ctrl: &Controller,
    ) -> Result<CreateTopicsResponse> {
        for (name, topic) in req.topics.into_iter() {
            if ctrl.store.topic_exists(&name)? {
                // TODO
            }

            let t = Self::create_topic(&name, topic, ctrl).await?;
            res.topics.insert(name, t);
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::broker::handler::create_topics::CreateTopicsHandler;
    use crate::broker::handler::test::new_controller;
    use crate::broker::handler::Handler;
    use crate::broker::state::topic::Topic;
    use crate::error::Result;
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::{CreateTopicsRequest, TopicName};
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (mut rx, ctrl) = new_controller();
        let mut req = CreateTopicsRequest::default();
        let topic_name = TopicName(StrBytes::from_str("Test"));
        req.topics
            .insert(topic_name.clone(), CreatableTopic::default());
        let (res, _) = tokio::join!(
            tokio::spawn(async move {
                Result::Ok(
                    CreateTopicsHandler::handle(req, CreateTopicsHandler::response(), &ctrl)
                        .await?,
                )
            }),
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
                )?)));
                Result::Ok(())
            }),
        );

        let res = res??;
        let name = res.topics.keys().next().unwrap();
        assert_eq!(&topic_name, name);
        Ok(())
    }
}
