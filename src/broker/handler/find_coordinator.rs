use crate::broker::handler::Handler;
use crate::broker::Broker;
use kafka_protocol::messages;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{FindCoordinatorRequest, FindCoordinatorResponse};

impl Handler<FindCoordinatorRequest> for Broker {
    async fn handle(
        &self,
        _: FindCoordinatorRequest,
        mut res: FindCoordinatorResponse,
    ) -> anyhow::Result<FindCoordinatorResponse> {
        let mut coordinator = Coordinator::default();
        coordinator.node_id = messages::BrokerId(self.config.id.0);
        coordinator.host = self.config.ip.to_string().into();
        coordinator.port = self.config.port as i32;

        res.coordinators.push(coordinator);
        Ok(res)
    }
}
