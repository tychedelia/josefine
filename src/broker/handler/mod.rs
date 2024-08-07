use std::fmt::Debug;

use kafka_protocol::protocol::Request;

use anyhow::Result;

mod api_versions;
mod create_topics;
mod find_coordinator;
mod leader_and_isr;
mod list_groups;
mod metadata;
mod produce;
mod test;

pub(crate) trait Handler<Req, Res = <Req as Request>::Response>: Debug
where
    Req: Request + Default + Debug + Send + 'static,
    Res: Default + Debug + Send,
{
    async fn handle(&self, req: Req, res: Res) -> Result<Res>;

    fn response() -> Res {
        Res::default()
    }
}
