use string::TryFrom;
use bytes::Bytes;
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use kafka_protocol::protocol::StrBytes;
use crate::broker::command::{Command, Controller};
use async_trait::async_trait;

pub struct MetadataCommand;

#[async_trait]
impl Command for MetadataCommand {
    type Request = MetadataRequest;
    type Response = MetadataResponse;

    async fn execute(_: Self::Request, ctrl: &Controller) -> crate::error::Result<Self::Response> {
        let mut res = Self::response()  ;
        res.brokers.insert(
            BrokerId(1),
            MetadataResponseBroker {
                host: StrBytes::from_str("[::1]"),
                port: 8844,
                rack: None,
                unknown_tagged_fields: Default::default(),
            },
        );
        res.controller_id = BrokerId(1);
        res.cluster_id = Some(StrBytes::from_str("josefine"));

        let topics = ctrl.broker.get_topics()?;
        for (name, topic) in topics.into_iter() {
            let t = MetadataResponseTopic {
                topic_id: topic.id,
                ..Default::default()
            };
            let s = StrBytes::try_from(Bytes::from(name)).unwrap();
            res.topics.insert(TopicName(s), t);
        }

        Ok(res)
    }
}