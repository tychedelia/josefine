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
use crate::raft::Entry;

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping {
    header: Header,
    id: NodeId,
    message: String,
}


#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Ping(Ping),
    AddNodeRequest(Node),
    AppendRequest(AppendRequest),
    //    AppendResponse(AppendResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
//    SnapshotRequest(SnapshotRequest),
//    SnapshotResponse(SnapshotResponse),
}

pub trait Rpc {
    fn heartbeat(&self, term: u64, index: u64, entries: Vec<Entry>);
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
    fn heartbeat(&self, _term: u64, _index: u64, _entries: Vec<Entry>) {
        unimplemented!()
    }

    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) {}
    fn request_vote(&self, _state: &State, _node_id: u32) {}

    fn ping(&self, _node_id: u32) {}

    fn get_header(&self) -> Header {
        unimplemented!()
    }

    fn add_self_to_cluster(&self, _address: &str) {
        unimplemented!()
    }
}

pub struct TpcRpc {
    config: RaftConfig,
    tx: Sender<Command>,
    nodes: NodeMap,
    log: Logger,
    pool: ThreadPool,
}

impl TpcRpc {
    fn get_stream(&self, node_id: NodeId) -> Option<TcpStream> {
        let node = &self.nodes.borrow()[&node_id];
        let address = format!("{}:{}", node.ip, node.port);
        match TcpStream::connect(address) {
            Ok(stream) => Some(stream),
            _ => None,
        }
    }

    pub fn new(config: RaftConfig, tx: Sender<Command>, nodes: NodeMap, log: Logger) -> TpcRpc {
        let rpc = TpcRpc {
            config,
            tx,
            nodes,
            log,
            pool: ThreadPool::new(5),
        };

        for node in &rpc.config.nodes {
            rpc.add_self_to_cluster(node);
        }

        rpc
    }
}

impl Rpc for TpcRpc {
    fn heartbeat(&self, term: u64, index: u64, entries: Vec<Entry>) {
        let req = AppendRequest {
            header: self.get_header(),
            term,
            leader: self.config.id,
            prev_entry: 0,
            prev_term: 0,
            entries,
            leader_index: index,
        };

        for (id, _) in self.nodes.borrow().iter() {
            let msg = serde_json::to_vec(&req).expect("Could not serialize message");
            if let Some(mut stream) = self.get_stream(*id) {
                match stream.write_all(&msg[..]) {
                    Err(_err) => { error!(self.log, "Could not write to node"; "node_id" => format!("{}", id)) }
                    _ => {}
                };
            }
        }
    }

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
        if let Some(mut stream) = self.get_stream(node_id) {
            match stream.write_all(&msg[..]) {
                Err(_err) => { error!(self.log, "Could not write to node"; "node_id" => format!("{}", node_id)) }
                _ => {}
            };
        }
    }

    fn get_header(&self) -> Header {
        Header {
            version: self.config.protocol_version
        }
    }

    fn add_self_to_cluster(&self, address: &str) {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let msg = Message::AddNodeRequest(Node {
                id: self.config.id,
                ip: self.config.ip,
                port: self.config.port,
            });
            let msg = serde_json::to_vec(&msg).expect("Couldn't serialize value");
            stream.write_all(&msg[..]);
        }
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

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendRequest {
    pub header: Header,

    // Current term and leader
    pub term: u64,
    pub leader: NodeId,

    // Previous state for validation
    pub prev_entry: u64,
    pub prev_term: u64,

    // Entries to append
    pub entries: Vec<Entry>,

    // Index on the leader
    pub leader_index: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendResponse {
    header: Header,

    term: u64,
    last_log: u64,

    success: bool,
}