use std::io::Write;
use std::net::Ipv4Addr;
use std::net::TcpStream;

use crate::config::RaftConfig;
use crate::raft::NodeId;
use crate::raft::State;
use crate::raft::Node;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use slog::Logger;
use slog::Drain;
use crate::raft::NodeMap;
use std::sync::mpsc::Sender;
use crate::raft::Command;
use threadpool::ThreadPool;

#[derive(Serialize, Deserialize, Debug)]
struct Ping {
    header: Header,
    id: NodeId,
    message: String,
}


#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Ping(Ping),
    AddNodeRequest(Node),
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
    fn get_header(&self) -> Header;
    fn add_self_to_cluster(&self, address: &str);
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

    fn get_header(&self) -> Header {
        unimplemented!()
    }

    fn add_self_to_cluster(&self, address: &str) {
        unimplemented!()
    }
}

pub struct TpcRpc {
    config: RaftConfig,
    tx: Sender<Command>,
    nodes: NodeMap,
    pool: ThreadPool,
}

impl TpcRpc {
    fn get_stream(&self, node_id: NodeId) -> TcpStream {
        let node = &self.nodes.borrow()[&node_id];
        let address = format!("{}:{}", node.ip, node.port);
        TcpStream::connect(address).expect("Couldn't connect to node")
    }

    pub fn new(config: RaftConfig, tx: Sender<Command>, nodes: NodeMap) -> TpcRpc {
        let rpc = TpcRpc {
            config,
            tx,
            nodes,
            pool: ThreadPool::new(5),
        };

        for node in &rpc.config.nodes {
            rpc.add_self_to_cluster(node);
        }

        rpc
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
        let ping = Ping {
            header: self.get_header(),
            id: node_id,
            message: "ping!".to_string(),
        };
        let msg = Message::Ping(ping);
        let msg = serde_json::to_vec(&msg).expect("Couldn't serialize value");
        self.get_stream(node_id).write_all(&msg[..]);
    }

    fn get_header(&self) -> Header {
        Header {
            version: self.config.protocol_version
        }
    }

    fn add_self_to_cluster(&self, address: &str) {
        let mut stream = TcpStream::connect(address).expect("Couldn't connect to node");
        let msg = Message::AddNodeRequest(Node {
            id: self.config.id,
            ip: self.config.ip,
            port: self.config.port,
        });
        let msg = serde_json::to_vec(&msg).expect("Couldn't serialize value");
        stream.write_all(&msg[..]);
    }
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    version: u32,
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    header: Header,

    term: u64,
    candidate_id: NodeId,

    last_index: u64,
    last_term: u64,
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteResponse {
    header: Header,

    term: u64,
    granted: bool,
}