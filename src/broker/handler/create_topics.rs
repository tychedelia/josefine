use crate::broker::fsm::Transition;
use crate::broker::handler::{Controller, Handler};
use crate::broker::state::topic::Topic;
use async_trait::async_trait;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};
use uuid::Uuid;

#[derive(Debug)]
pub struct CreateTopicsHandler;

#[async_trait]
impl Handler<CreateTopicsRequest> for CreateTopicsHandler {
    async fn handle(
        req: CreateTopicsRequest,
        mut res: CreateTopicsResponse,
        ctrl: &Controller,
    ) -> crate::error::Result<CreateTopicsResponse> {
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

#[cfg(test)]
mod tests {
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
