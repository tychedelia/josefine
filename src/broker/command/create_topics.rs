use crate::broker::command::{Command, Controller};
use crate::broker::fsm::Transition;
use crate::broker::model::topic::Topic;
use async_trait::async_trait;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};
use uuid::Uuid;

pub struct CreateTopicsCommand;

#[async_trait]
impl Command for CreateTopicsCommand {
    type Request = CreateTopicsRequest;
    type Response = CreateTopicsResponse;

    async fn execute(
        req: Self::Request,
        ctrl: &Controller,
    ) -> crate::error::Result<Self::Response> {
        let mut res = Self::response();
        for (name, _) in req.topics.into_iter() {
            let topic = Topic {
                id: Uuid::new_v4(),
                name: (*name).to_string(),
            };

            if ctrl.store.topic_exists(&name)? {
                // TODO
            }

            let _topic: Topic = bincode::deserialize(
                &ctrl
                    .client
                    .propose(Transition::EnsureTopic(topic).serialize()?)
                    .await?,
            )?;
            let res_topic = CreatableTopicResult::default();

            res.topics.insert(name, res_topic);
        }
        Ok(res)
    }
}
