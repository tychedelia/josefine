use crate::broker::handler::Handler;
use crate::broker::replica::Replica;
use crate::broker::state::partition::PartitionIdx;
use crate::broker::Broker;
use crate::broker::BrokerId;
use kafka_protocol::messages::{LeaderAndIsrRequest, LeaderAndIsrResponse};

impl Handler<LeaderAndIsrRequest> for Broker {
    async fn handle(
        &self,
        req: LeaderAndIsrRequest,
        res: LeaderAndIsrResponse,
    ) -> anyhow::Result<LeaderAndIsrResponse> {
        for ts in req.topic_states {
            for ps in ts.partition_states {
                let partition = self
                    .store
                    .get_partition(&ps.topic_name, PartitionIdx(ps.partition_index))?
                    .ok_or(anyhow::anyhow!("could not find partition"))?;
                let pid = partition.id;
                let replica = Replica::new(&self.config.data_dir, BrokerId(ps.leader.0), partition);
                self.replicas.add(pid, replica);
            }
        }

        Ok(res)
    }
}
