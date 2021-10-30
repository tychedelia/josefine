use std::net::SocketAddr;

use crate::error::Result;
use futures::FutureExt;
use tokio::net::TcpListener;

use crate::broker::tcp;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Message, StrBytes};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::broker::fsm::Transition;
use crate::broker::topic::Topic;
use crate::broker::broker::Broker;
use crate::logger::get_root_logger;
use crate::raft::client::RaftClient;
use bytes::Bytes;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use slog::Logger;
use string::TryFrom;
use uuid::Uuid;

pub struct Server {
    address: SocketAddr,
}

impl Server {
    pub fn new(address: SocketAddr) -> Self {
        Server { address }
    }

    pub async fn run(
        self,
        client: RaftClient,
        broker: Broker,
        shutdown: (
            tokio::sync::broadcast::Sender<()>,
            tokio::sync::broadcast::Receiver<()>,
        ),
    ) -> Result<()> {
        let log = get_root_logger();
        info!(log, "broker listening"; "address" => &self.address);
        let listener = TcpListener::bind(self.address).await?;
        let (in_tx, out_tx) = tokio::sync::mpsc::unbounded_channel();
        let (task, tcp_receiver) =
            tcp::receive_task(crate::logger::get_root_logger().new(o!()), listener, in_tx, shutdown.0.subscribe())
                .remote_handle();
        tokio::spawn(task);

        let (task, handle_messages) =
            handle_messages(log.new(o!()), client, broker, out_tx, shutdown.0.subscribe()).remote_handle();
        tokio::spawn(task);

        let (_, _) = tokio::try_join!(tcp_receiver, handle_messages)?;
        Ok(())
    }
}

async fn handle_messages(
    log: Logger,
    client: RaftClient,
    broker: Broker,
    mut out_tx: UnboundedReceiver<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,

            Some((msg, cb)) = out_tx.recv() => {
                debug!(log, "received message"; "msg" => format!("{:?}", msg));
                handle_message(&client, &broker, msg, cb).await.unwrap();
            }
        }
    }

    Ok(())
}

async fn handle_message(
    client: &RaftClient,
    broker: &Broker,
    msg: RequestKind,
    cb: oneshot::Sender<ResponseKind>,
) -> Result<()> {
    match msg {
        RequestKind::ApiVersionsRequest(_req) => {
            let mut res = ApiVersionsResponse::default();
            res.api_keys.insert(
                ApiKey::ProduceKey as i16,
                ApiVersion {
                    max_version: ProduceRequest::VERSIONS.max,
                    min_version: ProduceRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::FetchKey as i16,
                ApiVersion {
                    max_version: FetchRequest::VERSIONS.max,
                    min_version: FetchRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::ListOffsetsKey as i16,
                ApiVersion {
                    max_version: ListOffsetsRequest::VERSIONS.max,
                    min_version: ListOffsetsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::MetadataKey as i16,
                ApiVersion {
                    max_version: MetadataRequest::VERSIONS.max,
                    min_version: MetadataRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::LeaderAndIsrKey as i16,
                ApiVersion {
                    max_version: LeaderAndIsrRequest::VERSIONS.max,
                    min_version: LeaderAndIsrRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::StopReplicaKey as i16,
                ApiVersion {
                    max_version: StopReplicaRequest::VERSIONS.max,
                    min_version: StopReplicaRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::FindCoordinatorKey as i16,
                ApiVersion {
                    max_version: FindCoordinatorRequest::VERSIONS.max,
                    min_version: FindCoordinatorRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::JoinGroupKey as i16,
                ApiVersion {
                    max_version: JoinGroupRequest::VERSIONS.max,
                    min_version: JoinGroupRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::HeartbeatKey as i16,
                ApiVersion {
                    max_version: HeartbeatRequest::VERSIONS.max,
                    min_version: HeartbeatRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::ListGroupsKey as i16,
                ApiVersion {
                    max_version: LeaveGroupRequest::VERSIONS.max,
                    min_version: LeaveGroupRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::SyncGroupKey as i16,
                ApiVersion {
                    max_version: SyncGroupRequest::VERSIONS.max,
                    min_version: SyncGroupRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::CreateTopicsKey as i16,
                ApiVersion {
                    max_version: CreateTopicsRequest::VERSIONS.max,
                    min_version: CreateTopicsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::DeleteGroupsKey as i16,
                ApiVersion {
                    max_version: DescribeGroupsRequest::VERSIONS.max,
                    min_version: DescribeGroupsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::ListGroupsKey as i16,
                ApiVersion {
                    max_version: ListGroupsRequest::VERSIONS.max,
                    min_version: ListGroupsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::ApiVersionsKey as i16,
                ApiVersion {
                    max_version: ApiVersionsRequest::VERSIONS.max,
                    min_version: ApiVersionsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            res.api_keys.insert(
                ApiKey::DeleteTopicsKey as i16,
                ApiVersion {
                    max_version: DeleteTopicsRequest::VERSIONS.max,
                    min_version: DeleteTopicsRequest::VERSIONS.min,
                    ..Default::default()
                },
            );
            cb.send(ResponseKind::ApiVersionsResponse(res)).unwrap();
        }
        RequestKind::MetadataRequest(_req) => {
            let mut res = MetadataResponse::default();
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

            let topics = broker.get_topics()?;
            for (name, topic) in topics.into_iter() {
                let t = MetadataResponseTopic {
                    topic_id: topic.id,
                    ..Default::default()
                };
                let s = StrBytes::try_from(Bytes::from(name)).unwrap();
                res.topics.insert(TopicName(s), t);
            }

            cb.send(ResponseKind::MetadataResponse(res)).unwrap()
        }
        RequestKind::CreateTopicsRequest(req) => {
            let mut res = CreateTopicsResponse::default();
            for (name, _) in req.topics.into_iter() {
                let topic = Topic {
                    id: Uuid::new_v4(),
                    name: (*name).to_string(),
                };

                if broker.topic_exists(&name)? {
                    // TODO
                }

                let _topic: Topic = bincode::deserialize(
                    &client
                        .propose(Transition::EnsureTopic(topic).serialize()?)
                        .await?,
                )?;
                let res_topic = CreatableTopicResult::default();

                res.topics.insert(name, res_topic);
            }
            cb.send(ResponseKind::CreateTopicsResponse(res)).unwrap();
        }
        _ => panic!(),
    };
    Ok(())
}
