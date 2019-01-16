use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Error;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

use slog::Logger;

use crate::candidate::Candidate;
use crate::config::RaftConfig;
use crate::follower::Follower;
use crate::leader::Leader;
use crate::rpc::Rpc;
use threadpool::ThreadPool;
use std::time::Duration;
use std::time::Instant;

pub type NodeId = u32;

// Commands that can be applied to the state machine.
#[derive(Debug)]
pub enum Command {
    Tick,
    //
    AddNode(Node),
    // Our vote has been requested by another node.
    VoteRequest { term: u64, from: NodeId },
    // Vote (or not) for another node.
    VoteResponse { term: u64, from: NodeId, granted: bool },
    // Request from another node to append entries to our log.
    Append { term: u64, from: NodeId, entries: Vec<Entry> },
    // Heartbeat from another node.
    Heartbeat { term: u64, from: NodeId },
    // Timeout on an event (i.e. election).
    Timeout,
    // Don't do anything. TODO: Change to more useful health check or info command, or remove.
    Noop,
    Ping(NodeId),
}

pub trait Role {
    fn term(&mut self, term: u64);
}

// Defines all IO (i.e. persistence) related behavior. Making our implementation generic over
// IO is slightly annoying, but allows us, e.g., to implement different backend strategies for
// persistence, which makes it easier for testing and helps isolate the "pure logic" of the state
// machine from persistence concerns.
//
// Right now, this doesn't handle communication with other nodes. TODO: TBD.
pub trait Io {
    fn new() -> Self;
    fn append(&mut self, entries: &mut Vec<Entry>);
    fn heartbeat(&mut self, id: NodeId);
}

// An entry in the commit log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

// Simple IO impl used for mocking + testing.
pub struct MemoryIo {
    entries: Vec<Entry>
}

impl Io for MemoryIo {
    fn new() -> Self {
        MemoryIo { entries: Vec::new() }
    }

    fn append(&mut self, entries: &mut Vec<Entry>) {
        self.entries.append(entries);
    }

    fn heartbeat(&mut self, _id: NodeId) {
        unimplemented!()
    }
}

// Contains information about nodes in raft cluster.
#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub id: NodeId,
    pub ip: IpAddr,
    pub port: u32,
}

impl Node {
    pub fn new(id: NodeId, ip: IpAddr, port: u32) -> Node {
        Node {
            id,
            ip,
            port,
        }
    }
}

// Volatile and persistent state for all roles.
// NB: These could just be fields on the common Raft struct, but copying them is annoying.
#[derive(PartialEq, Clone, Copy)]
pub struct State {
    // update on storage
    pub current_term: u64,
    pub voted_for: Option<NodeId>,

    // volatile state
    pub commit_index: u64,
    pub last_applied: u64,

    // timers
    pub election_time: Option<Instant>,
    pub heartbeat_time: Option<Instant>,
    pub election_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub min_election_timeout: usize,
    pub max_election_timeout: usize,
}

impl State {
    pub fn new() -> State {
        State::default()
    }
}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            election_time: None,
            heartbeat_time: None,
            election_timeout: None,
            heartbeat_timeout: None,
            min_election_timeout: 10,
            max_election_timeout: 1000,
        }
    }
}

pub type NodeMap = Rc<RefCell<HashMap<NodeId, Node>>>;

// Contains state and logic common to all raft variants.
pub struct Raft<S: Role, I: Io, R: Rpc> {
    // The identifier for this node.
    pub id: NodeId,
    pub log: Logger,

    // Known nodes in the cluster.
    pub nodes: NodeMap,

    // Volatile and persistent state.
    pub state: State,

    // IO implementation.
    pub io: I,

    // Rpc implementation
    pub rpc: R,

    // Struct for role specific state + methods.
    pub role: S,
}

// Base methods for general operations (+ debugging and testing).
impl<S: Role, I: Io, R: Rpc> Raft<S, I, R> {
    pub fn add_node_to_cluster(&mut self, node: Node) {
        info!(self.log, "Adding node"; "node" => format!("{:?}", node));
        let node_id = node.id;
        self.nodes.borrow_mut().insert(node.id, node);
        self.rpc.ping(node_id);
    }

    pub fn term(&mut self, term: u64) {
        self.state.voted_for = None;
        self.state.current_term = term;

        self.role.term(term);
    }
}

// Since applying command to the state machine can potentially result in any state transition,
// the result that we get back needs to be general to the possible return types -- easiest
// way here is just to store the differently sized structs per state in an enum, which will be
// sized to the largest variant.
pub enum RaftHandle<I: Io, R: Rpc> {
    Follower(Raft<Follower, I, R>),
    Candidate(Raft<Candidate, I, R>),
    Leader(Raft<Leader, I, R>),
}

impl<I: Io, R: Rpc> RaftHandle<I, R> {
    pub fn new(config: RaftConfig, io: I, rpc: R, logger: Logger, nodes: NodeMap) -> RaftHandle<I, R> {
        let raft = Raft::new(config, io, rpc, Some(logger), Some(nodes));
        RaftHandle::Follower(raft.unwrap())
    }
}

impl<I: Io, R: Rpc> Apply<I, R> for RaftHandle<I, R> {
    fn apply(self, command: Command) -> Result<RaftHandle<I, R>, Error> {
        match self {
            RaftHandle::Follower(raft) => { raft.apply(command) }
            RaftHandle::Candidate(raft) => { raft.apply(command) }
            RaftHandle::Leader(raft) => { raft.apply(command) }
        }
    }
}

// Applying a command is the basic way the state machine is moved forward.
pub trait Apply<I: Io, R: Rpc> {
    // Apply a command to the raft state machine, which may result in a new raft state.
    fn apply(self, command: Command) -> Result<RaftHandle<I, R>, Error>;
}

