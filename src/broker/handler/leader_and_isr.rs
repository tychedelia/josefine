use kafka_protocol::messages::{LeaderAndIsrRequest, LeaderAndIsrResponse};
use kafka_protocol::protocol::Request;
use crate::broker::Broker;
use crate::broker::config::BrokerId;
use crate::broker::handler::Handler;
use crate::broker::replica::Replica;
use async_trait::async_trait;

#[async_trait]
impl Handler<LeaderAndIsrRequest> for Broker {

    async fn handle(&self, req: LeaderAndIsrRequest, res: LeaderAndIsrResponse) -> crate::error::Result<LeaderAndIsrResponse> {
        for ts in req.topic_states {
            for ps in ts.partition_states {
                let partition = self.store.get_partition(&ps.topic_name, ps.partition_index)?.expect("TODO: couldn't find partition");
                let pid = partition.id.clone();
                let replica = Replica::new(&self.config.data_dir, BrokerId(ps.leader.0), partition);
                self.replicas.add(pid, replica);
            }
        }

        Ok(res)
    }
}