use crate::broker::handler::{Controller, Handler};
use async_trait::async_trait;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Message;

#[derive(Debug)]
pub struct ApiVersionsHandler;

#[async_trait]
impl Handler<ApiVersionsRequest> for ApiVersionsHandler {
    async fn handle(
        _req: ApiVersionsRequest,
        mut res: ApiVersionsResponse,
        _: &Controller,
    ) -> crate::error::Result<ApiVersionsResponse> {
        res.api_keys.insert(
            ApiKey::ProduceKey as i16,
            ApiVersion {
                max_version: ProduceRequest::VERSIONS.max,
                min_version: ProduceRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::FetchKey as i16,
            ApiVersion {
                max_version: FetchRequest::VERSIONS.max,
                min_version: FetchRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::ListOffsetsKey as i16,
            ApiVersion {
                max_version: ListOffsetsRequest::VERSIONS.max,
                min_version: ListOffsetsRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::MetadataKey as i16,
            ApiVersion {
                max_version: MetadataRequest::VERSIONS.max,
                min_version: MetadataRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::LeaderAndIsrKey as i16,
            ApiVersion {
                max_version: LeaderAndIsrRequest::VERSIONS.max,
                min_version: LeaderAndIsrRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::StopReplicaKey as i16,
            ApiVersion {
                max_version: StopReplicaRequest::VERSIONS.max,
                min_version: StopReplicaRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::FindCoordinatorKey as i16,
            ApiVersion {
                max_version: FindCoordinatorRequest::VERSIONS.max,
                min_version: FindCoordinatorRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::JoinGroupKey as i16,
            ApiVersion {
                max_version: JoinGroupRequest::VERSIONS.max,
                min_version: JoinGroupRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::HeartbeatKey as i16,
            ApiVersion {
                max_version: HeartbeatRequest::VERSIONS.max,
                min_version: HeartbeatRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::ListGroupsKey as i16,
            ApiVersion {
                max_version: LeaveGroupRequest::VERSIONS.max,
                min_version: LeaveGroupRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::SyncGroupKey as i16,
            ApiVersion {
                max_version: SyncGroupRequest::VERSIONS.max,
                min_version: SyncGroupRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::CreateTopicsKey as i16,
            ApiVersion {
                max_version: CreateTopicsRequest::VERSIONS.max,
                min_version: CreateTopicsRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            ApiVersion {
                max_version: DescribeGroupsRequest::VERSIONS.max,
                min_version: DescribeGroupsRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::ListGroupsKey as i16,
            ApiVersion {
                max_version: ListGroupsRequest::VERSIONS.max,
                min_version: ListGroupsRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::ApiVersionsKey as i16,
            ApiVersion {
                max_version: ApiVersionsRequest::VERSIONS.max,
                min_version: ApiVersionsRequest::VERSIONS.min,
                ..Default::default()
            },
        );
        res.api_keys.insert(
            ApiKey::DeleteTopicsKey as i16,
            ApiVersion {
                max_version: DeleteTopicsRequest::VERSIONS.max,
                min_version: DeleteTopicsRequest::VERSIONS.min,
                ..Default::default()
            },
        );

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::ApiVersionsRequest;

    use crate::broker::handler::api_versions::ApiVersionsHandler;
    use crate::broker::handler::test::new_controller;
    use crate::broker::handler::Handler;

    use crate::error::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, ctrl) = new_controller();
        let req = ApiVersionsRequest::default();
        let _res = ApiVersionsHandler::handle(req, ApiVersionsHandler::response(), &ctrl).await?;
        Ok(())
    }
}
