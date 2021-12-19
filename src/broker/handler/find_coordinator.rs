use crate::kafka::util::ToStrBytes;
use async_trait::async_trait;
use kafka_protocol::messages;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{FindCoordinatorRequest, FindCoordinatorResponse};
use crate::broker::Broker;
use crate::broker::handler::Handler;

#[async_trait]
impl Handler<FindCoordinatorRequest> for Broker {
    async fn handle(
        &self,
        _: FindCoordinatorRequest,
        mut res: FindCoordinatorResponse,
    ) -> crate::error::Result<FindCoordinatorResponse> {
        let mut coordinator = Coordinator::default();
        coordinator.node_id = messages::BrokerId(self.config.id.0);
        coordinator.host = self.config.ip.to_string().to_str_bytes();
        coordinator.port = self.config.port as i32;

        res.coordinators.push(coordinator);
        Ok(res)
    }
}
