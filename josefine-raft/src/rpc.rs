use crate::raft::{Command, NodeId, Term};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub from: Address,
    pub to: Address,
    pub command: Command,
}

impl Message {
    pub fn new(from: Address, to: Address, command: Command) -> Message {
        return Message {
            from,
            to,
            command,
        };
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Proposal(pub Vec<u8>);

impl Proposal {
    pub fn get(self) -> Vec<u8> {
        self.0
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    State(Vec<u8>),
}