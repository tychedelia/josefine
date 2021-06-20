use uuid::Uuid;

enum Types {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    VarInt(i32),
    VarLong(i64),
    Uuid(Uuid),
    Float64(f64),
}

enum ErrorCodes {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
}

enum ApiKey {
    Produce = 1,
    Fetch,
    ListOffsets,
    Metadata,
    LeaderAndIsr,
    StopReplica,
    UpdateMetadata,
    ControlledShutdown,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    DescribeGroups,
    ListGroups,
    SaslHandshake,
    ApiVersions,
    CreateTopics,
    DeleteTopics,
    DeleteRecords,
    InitProducerId,
    OffsetForLeaderEpoch,
    AddPartitionsToTxn,
    AddOffsetsToTxn,
    EndTxn,
    WriteTxnMarkers,
    TxnOffsetCommit,
    DescribeAcls,
    CreateAcls,
    DeleteAcls,
    DescribeConfigs,
    AlterConfigs,
    AlterReplicaLogDirs,
    DescribeLogDirs,
    SaslAuthenticate,
    CreatePartitions,
    CreateDelegationToken,
    RenewDelegationToken,
    ExpireDelegationToken,
    DescribeDelegationToken,
    DeleteGroups,
    ElectLeaders,
    IncrementalAlterConfigs,
    AlterPartitionReassignments,
    ListPartitionReassignments,
    OffsetDelete,
    DescribeClientQuotas,
    AlterClientQuotas,
    DescribeUserScramCredentials,
    AlterUserScramCredentials,
    AlterIsr,
    UpdateFeatures,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestHeader<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}


#[derive(Debug, Serialize, Deserialize)]
pub enum MessageKind<'a> {
    ApiVersions { header: RequestHeader<'a>, client_software_name: &'a str, client_software_version: &'a str  }
}
