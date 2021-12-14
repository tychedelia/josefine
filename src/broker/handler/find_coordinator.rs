use crate::broker::handler::{Controller, Handler};
use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use kafka_protocol::messages;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};

#[derive(Debug)]
pub struct FindCoordinatorHandler;

#[async_trait]
impl Handler<FindCoordinatorRequest> for FindCoordinatorHandler {
    async fn handle(
        _: FindCoordinatorRequest,
        mut res: FindCoordinatorResponse,
        ctrl: &Controller,
    ) -> crate::error::Result<FindCoordinatorResponse> {
        let mut coordinator = Coordinator::default();
        coordinator.node_id = messages::BrokerId(ctrl.config.id.0);
        coordinator.host = ctrl.config.ip.to_string().to_str_bytes();
        coordinator.port = ctrl.config.port as i32;

        res.coordinators.push(coordinator);
        Ok(res)
    }
}
