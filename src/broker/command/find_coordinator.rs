use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use crate::broker::command::{Command, Controller};
use async_trait::async_trait;
use kafka_protocol::protocol::StrBytes;
use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use crate::kafka::util::ToStrBytes;

pub struct FindCoordinatorCommand;

#[async_trait]
impl Command for FindCoordinatorCommand {
    type Request = FindCoordinatorRequest;
    type Response = FindCoordinatorResponse;

    async fn execute(req: Self::Request, ctrl: &Controller) -> crate::error::Result<Self::Response> {
        let mut res = Self::response();

        let mut coordinator = Coordinator::default();
        coordinator.node_id = BrokerId(ctrl.config.id);
        coordinator.host = ctrl.config.ip.to_string().to_str_bytes();
        coordinator.port = ctrl.config.port as i32;

        res.coordinators.push(coordinator);
            Ok(res)
    }
}