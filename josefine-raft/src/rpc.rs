use crate::raft::{Command, Entry, LogIndex, NodeId, Term};

#[derive(Debug, Serialize, Deserialize)]
pub enum Address {
    /// Broadcast to all peers.
    Peers,
    /// A remote peer.
    Peer(NodeId),
    /// The local node.
    Local,
    /// A local client.
    Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub term: Term,
    pub from: Address,
    pub to: Address,
    pub command: Command,
}

impl Message {
    pub fn new(term: Term, from: Address, to: Address, command: Command) -> Message {
        return Message {
            term,
            from,
            to,
            command,
        };
    }
}
