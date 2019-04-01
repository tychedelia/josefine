use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::io;
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

use crate::config::RaftConfig;
use crate::raft::{ApplyStep, Command};
use crate::raft::Entry;
use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::NodeMap;
use crate::raft::State;

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
    AddNodeRequest(SocketAddr),
    AppendRequest(AppendRequest),
    AppendResponse(AppendResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    //    SnapshotRequest(SnapshotRequest),
//    SnapshotResponse(SnapshotResponse),
    InfoRequest(InfoRequest),
    InfoResponse(InfoResponse),
}

pub trait Rpc: Send + 'static {
    fn heartbeat(&self, node_id: NodeId, term: u64, index: u64, entries: &[Entry]) -> Result<(), RpcError>;
    fn respond_vote(&self, state: &State, candidate_id: NodeId, granted: bool);
    fn request_vote(&self, state: &State, node_id: NodeId);
    fn respond_append(&self, node_id: NodeId, term: u64, index: u64) -> Result<(), RpcError>;
    fn ping(&self, node_id: NodeId);
    fn get_header(&self) -> Header;
    fn add_self_to_cluster(&self, address: &str) -> Result<(), failure::Error>;
}

pub struct NoopRpc {}

impl NoopRpc {
    #[allow(dead_code)]
    pub fn new() -> NoopRpc {
        NoopRpc {}
    }
}

impl Default for NoopRpc {
    fn default() -> Self {
        NoopRpc {}
    }
}

impl Rpc for NoopRpc {
    fn heartbeat(&self, _node_id: NodeId, _term: u64, _index: u64, _entries: &[Entry]) -> Result<(), RpcError> { Ok(()) }
    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) {}
    fn request_vote(&self, _state: &State, _node_id: u32) {}
    fn respond_append(&self, _node_id: NodeId, _term: u64, _index: u64) -> Result<(), RpcError> { Ok(()) }
    fn ping(&self, _node_id: u32) {}
    fn get_header(&self) -> Header {
        unimplemented!()
    }
    fn add_self_to_cluster(&self, _address: &str) -> Result<(), failure::Error> { Ok(()) }
}

pub struct TpcRpc {
    config: RaftConfig,
    tx: Sender<ApplyStep>,
    nodes: NodeMap,
    log: Logger,
}

impl TpcRpc {
    fn msg_as_bytes(msg: &Message) -> io::Result<Vec<u8>> {
        Ok(serde_json::to_vec(msg)?)
    }

    fn get_stream(&self, node_id: NodeId) -> Result<TcpStream, failure::Error> {
        let node = &self.nodes.read().unwrap()[&node_id];
        TcpStream::connect(node.addr)
            .map_err(|e| e.into())
    }

    pub fn new(config: RaftConfig, tx: Sender<ApplyStep>, nodes: NodeMap, log: Logger) -> TpcRpc {
        TpcRpc {
            config,
            tx,
            nodes,
            log,
        }
    }
}

#[derive(Fail, Debug)]
pub enum RpcError {
    #[fail(display = "A connection error occurred.")]
    Connection(#[fail(cause)] failure::Error)
}

impl Rpc for TpcRpc {
    fn heartbeat(&self, node_id: NodeId, term: u64, index: u64, entries: &[Entry]) -> Result<(), RpcError> {
        let req = AppendRequest {
            header: self.get_header(),
            term,
            leader: self.config.id,
            prev_entry: 0,
            prev_term: 0,
            entries: entries.to_vec(),
            leader_index: index,
        };

        let msg = Message::AppendRequest(req);
        let msg = Self::msg_as_bytes(&msg).expect("Couldn't serialize message");

        self.get_stream(node_id)
            .and_then(|mut stream| stream.write_all(&msg[..])
                .map_err(|error| error.into()))
            .map_err(RpcError::Connection)?;

        Ok(())
    }


    fn respond_vote(&self, _state: &State, _candidate_id: u32, _granted: bool) {
        unimplemented!()
    }

    fn request_vote(&self, _state: &State, _node_id: u32) {
        unimplemented!()
    }

    fn respond_append(&self, node_id: NodeId, term: u64, index: u64) -> Result<(), RpcError> {
        let res = AppendResponse {
            header: self.get_header(),
            term,
            last_log: index,
            success: true
        };

        let msg = Message::AppendResponse(res);
        let msg = Self::msg_as_bytes(&msg).expect("Couldn't serialize message");

        self.get_stream(node_id)
            .and_then(|mut stream| stream.write_all(&msg[..])
                .map_err(|error| error.into()))
            .map_err(RpcError::Connection)?;

        Ok(())
    }

    fn ping(&self, node_id: u32) {
        let ping = Ping {
            header: self.get_header(),
            id: node_id,
            message: "ping!".to_string(),
        };
        let msg = Message::Ping(ping);
        let msg = Self::msg_as_bytes(&msg).expect("Couldn't serialize value");
        if let Ok(mut stream) = self.get_stream(node_id) {
            if let Err(_err) = stream.write_all(&msg[..]) {
                error!(self.log, "Could not write to node"; "node_id" => format!("{}", node_id));
            };
        }
    }

    fn get_header(&self) -> Header {
        Header {
            node_id: self.config.id,
            version: self.config.protocol_version
        }
    }

    fn add_self_to_cluster(&self, address: &str) -> Result<(), failure::Error> {
        info!(self.log, "Adding self to cluster"; "addr" => address);
        let mut stream = TcpStream::connect(address)?;
        let msg = Message::AddNodeRequest(SocketAddr::new(self.config.ip, self.config.port));
        let msg = Self::msg_as_bytes(&msg).expect("Couldn't serialize value");
        stream.write_all(&msg[..])?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub node_id: NodeId,
    pub version: u32,
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
    pub header: Header,

    pub term: u64,
    pub last_log: u64,

    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfoRequest {}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfoResponse {
    node_id: NodeId,
}
