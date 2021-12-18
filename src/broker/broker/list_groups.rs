use crate::broker::broker::{Broker, Handler};
use async_trait::async_trait;
use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};

#[async_trait]
impl Handler<ListGroupsRequest> for Broker {
    async fn handle(
        &self,
        _req: ListGroupsRequest,
        res: ListGroupsResponse,
    ) -> crate::error::Result<ListGroupsResponse> {
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};

    use crate::broker::broker::test::new_broker;
    use crate::broker::broker::Handler;

    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let req = ListGroupsRequest::default();
        let _res = broker.handle(req, ListGroupsResponse::default()).await?;
        Ok(())
    }
}
