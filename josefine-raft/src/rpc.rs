use std::io::Write;
use std::net::Ipv4Addr;
use std::net::TcpStream;

use crate::config::RaftConfig;
use crate::raft::NodeId;
use crate::raft::State;

#[allow(dead_code)]
pub enum Message {
    //    AppendRequest(AppendRequest),
//    AppendResponse(AppendResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
//    SnapshotRequest(SnapshotRequest),
//    SnapshotResponse(SnapshotResponse),
}

pub trait Rpc {
    fn respond_vote(&self, state: &State, candidate_id: NodeId, granted: bool);
    fn request_vote(&self, state: &State, node_id: NodeId);
    fn ping(&self, node_id: NodeId);
}

pub struct NoopRpc {}

impl NoopRpc {
    #[allow(dead_code)]
    pub fn new() -> NoopRpc {
        NoopRpc {}
    }
}

impl Rpc for NoopRpc {
    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) {}
    fn request_vote(&self, _state: &State, _node_id: u32) {}

    fn ping(&self, _node_id: u32) {}
}

pub struct TpcRpc {
    config: RaftConfig,
}

impl TpcRpc {
    fn get_stream(&self, node_id: NodeId) -> TcpStream {
        let ip = Ipv4Addr::from(node_id);
        let address = format!("{}:{}", ip, self.config.port);
        TcpStream::connect(address).unwrap()
    }

    pub fn new(config: RaftConfig) -> TpcRpc {
        TpcRpc {
            config
        }
    }
}

impl Rpc for TpcRpc {
    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) {
        unimplemented!()
    }

    fn request_vote(&self, _state: &State, _node_id: u32) {
        unimplemented!()
    }

    fn ping(&self, node_id: u32) {
        self.get_stream(node_id).write_all("PING".as_bytes()).unwrap();
    }
}

#[allow(dead_code)]
pub struct Header {
    version: u32,
}

#[allow(dead_code)]
pub struct VoteRequest {
    header: Header,

    term: u64,
    candidate_id: NodeId,

    last_index: u64,
    last_term: u64,
}

#[allow(dead_code)]
pub struct VoteResponse {
    header: Header,

    term: u64,
    granted: bool,
}