use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};
use crate::broker::command::{Command, Controller};
use async_trait::async_trait;

pub struct ListGroupsCommand;

#[async_trait]
impl Command for ListGroupsCommand {
    type Request = ListGroupsRequest;
    type Response = ListGroupsResponse;

    async fn execute(_req: Self::Request, _ctrl: &Controller) -> crate::error::Result<Self::Response> {
        let res = Self::response();
        Ok(res)
    }
}