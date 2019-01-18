use std::cell::RefCell;
use std::collections::HashMap;
use std::io::BufReader;
use std::io::Write;
use std::iter;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::sync::RwLock;
use std::thread;

use slog::Drain;
use slog::Logger;
use threadpool::ThreadPool;

use crate::config::RaftConfig;
use crate::raft::Command;
use crate::raft::Entry;
use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::NodeMap;
use crate::raft::State;
use std::error::Error;

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
    fn add_self_to_cluster(&self, address: &str) -> Result<(), failure::Error>;
}

pub struct NoopRpc { }

impl NoopRpc {
    #[allow(dead_code)]
    pub fn new() -> NoopRpc {
        NoopRpc { }
    }
}

impl Rpc for NoopRpc {
    fn heartbeat(&self, _term: u64, _index: u64, _entries: Vec<Entry>) {
        unimplemented!()
    }

    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) { }
    fn request_vote(&self, _state: &State, _node_id: u32) { }

    fn ping(&self, _node_id: u32) { }

    fn get_header(&self) -> Header {
        unimplemented!()
    }

    fn add_self_to_cluster(&self, _address: &str) -> Result<(), failure::Error>     {
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
    fn get_stream(&self, node_id: NodeId) -> Result<TcpStream, failure::Error> {
        let node = &self.nodes.read().unwrap()[&node_id];
        TcpStream::connect(node.addr)
            .map_err(|e| e.into())
    }

    pub fn new(config: RaftConfig, tx: Sender<Command>, nodes: NodeMap, log: Logger) -> TpcRpc {
        TpcRpc {
            config,
            tx,
            nodes,
            log,
            pool: ThreadPool::new(5),
        }
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

        let msg = Message::AppendRequest(req);
        let msg = serde_json::to_vec(&msg).expect("Couldn't serialize json");

        for (id, _) in self.nodes.read().unwrap().iter() {
            if id == &self.config.id {
                continue;
            }

            self.get_stream(*id)
                .and_then(|mut stream| stream.write_all(&msg[..])
                    .map_err(|err| err.into()));
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
        if let Ok(mut stream) = self.get_stream(node_id) {
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

    fn add_self_to_cluster(&self, address: &str) -> Result<(), failure::Error> {
        info!(self.log, "Adding self to cluster"; "addr" => address);
        let mut stream = TcpStream::connect(address)?;
        let msg = Message::AddNodeRequest(Node {
            id: self.config.id,
            addr: SocketAddr::new(self.config.ip, self.config.port),
        });
        let msg = serde_json::to_vec(&msg).expect("Couldn't serialize value");
        stream.write_all(&msg[..])?;
        Ok(())
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
    pub header: Header,

    pub term: u64,
    pub candidate_id: NodeId,

    pub last_index: u64,
    pub last_term: u64,
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
