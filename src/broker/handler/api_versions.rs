use crate::broker::handler::Handler;
use crate::broker::Broker;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Message;

fn api_version<T: Message>() -> ApiVersion {
    let mut v = ApiVersion::default();
    v.max_version = T::VERSIONS.max;
    v.min_version = T::VERSIONS.min;
    v
}

impl Handler<ApiVersionsRequest> for Broker {
    async fn handle(
        &self,
        _req: ApiVersionsRequest,
        mut res: ApiVersionsResponse,
    ) -> anyhow::Result<ApiVersionsResponse> {
        res.api_keys.insert(
            ApiKey::ProduceKey as i16,
            api_version::<ProduceRequest>(),
        );
        res.api_keys.insert(
            ApiKey::FetchKey as i16,
            api_version::<FetchRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ListOffsetsKey as i16,
            api_version::<ListOffsetsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::MetadataKey as i16,
            api_version::<MetadataRequest>(),
        );
        res.api_keys.insert(
            ApiKey::LeaderAndIsrKey as i16,
            api_version::<LeaderAndIsrRequest>(),
        );
        res.api_keys.insert(
            ApiKey::StopReplicaKey as i16,
            api_version::<StopReplicaRequest>(),
        );
        res.api_keys.insert(
            ApiKey::FindCoordinatorKey as i16,
            api_version::<FindCoordinatorRequest>(),
        );
        res.api_keys.insert(
            ApiKey::JoinGroupKey as i16,
            api_version::<JoinGroupRequest>(),
        );
        res.api_keys.insert(
            ApiKey::HeartbeatKey as i16,
            api_version::<HeartbeatRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ListGroupsKey as i16,
            api_version::<ListGroupsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::SyncGroupKey as i16,
            api_version::<SyncGroupRequest>(),
        );
        res.api_keys.insert(
            ApiKey::CreateTopicsKey as i16,
            api_version::<CreateTopicsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            api_version::<DeleteGroupsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ListGroupsKey as i16,
            api_version::<ListGroupsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ApiVersionsKey as i16,
            api_version::<ApiVersionsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::DeleteTopicsKey as i16,
            api_version::<DeleteTopicsRequest>(),
        );
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};

    use crate::broker::handler::test::new_broker;
    use crate::broker::handler::Handler;
    use anyhow::Result;

    #[tokio::test]
    async fn execute() -> Result<()> {
        let (_rx, broker) = new_broker();
        let req = ApiVersionsRequest::default();
        let _res = broker.handle(req, ApiVersionsResponse::default()).await?;
        Ok(())
    }
}
