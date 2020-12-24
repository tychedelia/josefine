use std::collections::HashMap;

use config::Config;
use futures::stream::Stream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::Duration;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::error::Result;
use crate::raft::{Command, Entry, LogIndex, Node, NodeId, NodeMap, Term};

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
