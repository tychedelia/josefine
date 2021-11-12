use crate::broker::command::{Command, Controller};
use async_trait::async_trait;
use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};

pub struct ListGroupsCommand;

#[async_trait]
impl Command for ListGroupsCommand {
    type Request = ListGroupsRequest;
    type Response = ListGroupsResponse;

    async fn execute(
        _req: Self::Request,
        _ctrl: &Controller,
    ) -> crate::error::Result<Self::Response> {
        let res = Self::response();
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::ListGroupsRequest;
    
    use crate::broker::command::Command;
    use crate::broker::command::list_groups::ListGroupsCommand;
    use crate::broker::command::test::new_controller;
    
    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, ctrl) = new_controller();
        let req = ListGroupsRequest::default();
        let _res = ListGroupsCommand::execute(req, &ctrl).await?;
        Ok(())
    }
}