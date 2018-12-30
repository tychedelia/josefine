use std::io::Error;
use std::collections::HashMap;
use crate::leader::Leader;
use crate::follower::Follower;
use crate::candidate::Candidate;
use std::sync::mpsc::{Sender, Receiver};

// Commands that can be applied to the state machine.
#[derive(Debug)]
pub enum Command {
    // Our vote has been requested by another node.
    RequestVote { term: u64, from: u64 },
    // Vote (or not) for another node.
    Vote { term: u64, from: u64, voted: bool },
    // Request from another node to append entries to our log.
    Append { term: u64, from: u64, entries: Vec<Entry> },
    // Heartbeat from another node.
    Heartbeat { term: u64, from: u64 },
    // Timeout on an event (i.e. election).
    Timeout,
    // Don't do anything. TODO: Change to more useful health check or info command, or remove.
    Noop,
}

// Possible states in the raft state machine.
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// Defines all IO (i.e. persistence) related behavior. Making our implementation generic over
// IO is slightly annoying, but allows us, e.g., to implement different backend strategies for
// persistence, which makes it easier for testing and helps isolate the "pure logic" of the state
// machine from persistence concerns.
//
// Right now, this doesn't handle communication with other nodes. TODO: TBD.
pub trait IO {
    fn new() -> Self;
    fn append(&mut self, mut entries: Vec<Entry>);
    fn heartbeat(&mut self, id: u64);
}

// An entry in the commit log.
#[derive(Debug)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

// Simple IO impl used for mocking + testing.
pub struct MemoryIO {
    entries: Vec<Entry>
}

impl IO for MemoryIO {
    fn new() -> Self {
        MemoryIO { entries: Vec::new() }
    }

    fn append(&mut self, mut entries: Vec<Entry>) {
        self.entries.append(&mut entries);
    }

    fn heartbeat(&mut self, id: u64) {
        unimplemented!()
    }
}

// Contains information about nodes in raft cluster.
pub struct Node {
    pub id: u64,
    pub address: String,
}

impl Node {
    pub fn new(id: u64) -> Node {
        Node {
            id,
            address: String::new(),
        }
    }
}

// Volatile and persistent state for all roles.
// NB: These could just be fields on the common Raft struct, but copying them is annoying.
#[derive(PartialEq, Clone, Copy)]
pub struct State {
    // update on storage
    pub current_term: u64,
    pub voted_for: u64,

    // volatile state
    pub commit_index: u64,
    pub last_applied: u64,

    // timers
    pub election_time: usize,
    pub heartbeat_time: usize,
    pub election_timeout: usize,
    pub heartbeat_timeout: usize,
    pub min_election_timeout: usize,
    pub max_election_timeout: usize,
}

impl State {
    pub fn new() -> State {
        State {
            current_term: 0,
            voted_for: 0,
            commit_index: 0,
            last_applied: 0,
            election_time: 0,
            heartbeat_time: 0,
            election_timeout: 0,
            heartbeat_timeout: 0,
            min_election_timeout: 0,
            max_election_timeout: 0,
        }
    }
}

// Contains state and logic common to all raft variants.
pub struct Raft<S, T: IO> {
    // The identifier for this node.
    pub id: u64,

    // Messaging:
    // Outbox stores messages from this node to send to other nodes.
    // TODO: API? Is this necessary, or can be embedded in IO trait?
    pub outbox: Receiver<Command>,
    // Channel for registering messages that should be sent to other nodes.
    pub sender: Sender<Command>,

    // Known nodes in the cluster.
    pub cluster: Vec<Node>,

    // Volatile and persistent state.
    pub state: State,

    // IO implementation.
    pub io: T,

    // Flag for testing state
    // TODO: Necessary?
    pub role: Role,

    // Struct for role specific state + methods.
    // TODO: Better name for this field
    pub inner: S,
}

// Base methods for general operations (+ debugging and testing).
impl <S, T: IO> Raft<S, T> {
    pub fn add_node_to_cluster(&mut self, node: Node) {
        self.cluster.push(node);
    }

    pub fn get_term(command: &Command) -> Option<u64> {
        match command {
            Command::RequestVote { term, .. } => Some(term.clone()),
            Command::Vote { term, .. } => Some(term.clone()),
            Command::Append { term, .. } => Some(term.clone()),
            Command::Heartbeat { term, .. } => Some(term.clone()),
            Command::Timeout => None,
            Command::Noop => None,
        }
    }
}

// Since applying command to the state machine can potentially result in any state transition,
// the result that we get back needs to be general to the possible return types -- easiest
// way here is just to store the differently sized structs per state in an enum, which will be
// sized to the largest variant.
pub enum ApplyResult<T: IO> {
    Follower(Raft<Follower, T>),
    Candidate(Raft<Candidate, T>),
    Leader(Raft<Leader, T>),
}

// Applying a command is the basic way the state machine is moved forward.
// TODO: I'd like to be able to limit the applicable commands per variant using the type system.
pub trait Apply<T: IO> {
    // Apply a command to the raft state machine, which may result in a new raft state.
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error>;
}

