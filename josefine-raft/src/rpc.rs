use std::collections::HashMap;
use std::io::Write;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Mutex;

use crate::config::Config;
use crate::raft::NodeId;
use crate::raft::Raft;
use crate::raft::State;

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
    pub fn new() -> NoopRpc {
        NoopRpc {}
    }
}

impl Rpc for NoopRpc {
    fn respond_vote(&self, state: &State, candidate_id: u32, granted: bool) {}
    fn request_vote(&self, state: &State, node_id: u32) {}

    fn ping(&self, node_id: u32) {}
}

pub type ChannelMap = Arc<Mutex<HashMap<NodeId, Sender<Message>>>>;

pub struct TpcRpc {
    config: Config,
}

impl TpcRpc {
    fn get_stream(&self, node_id: NodeId) -> TcpStream {
        let ip = Ipv4Addr::from(node_id);
        let address = format!("{}:{}", ip, self.config.port);
        TcpStream::connect(address).unwrap()
    }

    pub fn new(config: Config) -> TpcRpc {
        TpcRpc {
            config
        }
    }
}

impl Rpc for TpcRpc {
    fn respond_vote(&self, state: &State, candidate_id: u32, granted: bool) {
        unimplemented!()
    }

    fn request_vote(&self, state: &State, node_id: u32) {
        unimplemented!()
    }

    fn ping(&self, node_id: u32) {
        self.get_stream(node_id).write_all("PING".as_bytes());
    }
}

pub struct Header {
    version: u32,
}

pub struct VoteRequest {
    header: Header,

    term: u64,
    candidate_id: NodeId,

    last_index: u64,
    last_term: u64,
}

pub struct VoteResponse {
    header: Header,

    term: u64,
    granted: bool,
}