use crate::raft::Raft;
use crate::raft::NodeId;
use crate::raft::State;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use crate::config::Config;

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
}

pub struct MemoryRpc {
    sender: Sender<Message>,
    config: Config,
}

impl MemoryRpc {
    pub fn new(config: Config, sender: Sender<Message>) -> MemoryRpc {
        MemoryRpc {
            sender,
            config,
        }
    }

    fn header(&self) -> Header {
        Header {
            version: self.config.protocol_version,
        }
    }
}

pub struct Header {
    version: u32,
}

impl Rpc for MemoryRpc {
    fn respond_vote(&self, state: &State, candidate_id: u32, granted: bool) {
        let res = VoteResponse {
            header: self.header(),
            term: state.current_term,
            granted
        };

        self.sender.send(Message::VoteResponse(res));
    }

    fn request_vote(&self, state: &State, node_id: u32) {
        let res = VoteRequest {
            header: self.header(),
            term: state.current_term,
            candidate_id: self.config.id,
            last_index: state.commit_index,
            last_term: state.current_term,
        };

        self.sender.send(Message::VoteRequest(res));
    }
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