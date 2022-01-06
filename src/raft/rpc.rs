use crate::raft::{Command, NodeId};
use std::fmt::{Debug, Display, Formatter};

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
        Message { from, to, command }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Proposal(Vec<u8>);

impl Proposal {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn get(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Response(Vec<u8>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseError {}

impl Display for ResponseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResponseError")
    }
}

impl std::error::Error for ResponseError {}

impl Response {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn get(self) -> Vec<u8> {
        self.0
    }
}
