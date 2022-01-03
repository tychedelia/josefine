use std::fmt::Debug;

use async_trait::async_trait;
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

#[async_trait]
pub(crate) trait Handler<Req, Res = <Req as Request>::Response>: Debug
where
    Req: Request + Default + Debug + Send + 'static,
    Res: Default + Debug + Send,
{
    #[tracing::instrument]
    async fn do_handle(&self, req: Req) -> Result<Res> {
        tracing::debug!("executing request");
        self.handle(req, Self::response()).await
    }

    async fn handle(&self, req: Req, mut res: Res) -> Result<Res>;

    fn response() -> Res {
        Res::default()
    }
}
