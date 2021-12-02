use crate::broker::handler::{Controller, Handler};
use async_trait::async_trait;
use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};

#[derive(Debug)]
pub struct ListGroupsHandler;

#[async_trait]
impl Handler<ListGroupsRequest> for ListGroupsHandler {
    async fn handle(
        _req: ListGroupsRequest,
        res: ListGroupsResponse,
        _ctrl: &Controller,
    ) -> crate::error::Result<ListGroupsResponse> {
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::ListGroupsRequest;

    use crate::broker::handler::list_groups::ListGroupsHandler;
    use crate::broker::handler::test::new_controller;
    use crate::broker::handler::Handler;

    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, ctrl) = new_controller();
        let req = ListGroupsRequest::default();
        let _res = ListGroupsHandler::handle(req, ListGroupsHandler::response(), &ctrl).await?;
        Ok(())
    }
}
