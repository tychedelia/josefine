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
use std::sync::RwLock;
use std::ops::Index;
use std::sync::RwLockReadGuard;
use std::net::SocketAddr;
use std::sync::mpsc::Sender;
use crate::io::Io;

/// An id that uniquely identifies this instance of Raft.
pub type NodeId = u32;

/// Commands that can be applied to the state machine.
#[derive(Debug)]
pub enum Command {
    /// Called once at beginning to bootstrap the state machine.
    Start,
    /// Move the state machine forward.
    Tick,
    /// Add a node to the list of known nodes.
    AddNode(SocketAddr),
    /// Request that this instance vote for the provide node.
    VoteRequest {
        /// The term of the candidate.
        term: u64,
        /// The id of the candidate requesting the vote.
        candidate_id: NodeId,
        ///
        last_term: u64,
        ///
        last_index: u64,
    },
    /// Respond to a vote from another node.
    VoteResponse {
        /// The term of the voter.
        term: u64,
        /// The id of the voter responding to the vote.
        candidate_id: NodeId,
        /// Whether the vote was granted to the candidate.
        granted: bool,
    },
    /// Request from another node to append entries to our log.
    AppendEntries {
        /// The term of the node requesting entries be appended to our commit log.
        term: u64,
        /// The id of the node sending entries.
        leader_id: NodeId,

        /// The entries to append to our commit log.
        entries: Vec<Entry>,
    },
    AppendResponse {
        node_id: NodeId,
        term: u64,
        index: u64,
    },
    /// Heartbeat from another node.
    Heartbeat {
        /// The term of the node sending a heartbeat.
        term: u64,
        /// The id of the node sending a heartbeat.
        leader_id: NodeId,
    },
    /// Timeout on an event (i.e. election).
    Timeout,
    /// Don't do anything.
    Noop,
    /// Say hello to another node.
    Ping(NodeId),
}

/// Shared behavior that all roles of the state machine must implement.
pub trait Role {
    /// Set the term for the node, reseting the current election.
    fn term(&mut self, term: u64);
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EntryType {
    Entry,
    Config,
}

/// An entry in the commit log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    /// The type of the entry
    pub entry_type: EntryType,
    /// The term of the entry.
    pub term: u64,
    /// The index of the entry within the commit log.
    pub index: u64,
    /// The data of the entry in raw bytes.
    pub data: Vec<u8>,
}

/// Contains information about nodes in raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Node {
    /// The id of the node.
    pub id: NodeId,
    /// The socket address for the TCP connection.
    pub addr: SocketAddr,
}

impl Node {
}

/// Volatile and persistent state that is common to all roles.
// NB: These could just be fields on the common Raft struct, but copying them is annoying.
#[derive(PartialEq, Clone, Copy)]
pub struct State {
    /// The current term of the state machine that is incremented as certain commands are applied
    /// to the state machine. The term of the Raft instance is used in determining leadership. For
    /// example, when a node receives an append entries request from a node with a higher term,
    /// the node will transition to follower and acknowledge that node as the leader.
    pub current_term: u64,
    /// Who the node has voted for in the current election.
    pub voted_for: Option<NodeId>,

    /// The current commit index of the replicated log.
    pub commit_index: u64,
    ///
    pub last_applied: u64,

    /// The time the election was started.
    pub election_time: Option<Instant>,
    /// The timeout for the current election.
    pub election_timeout: Option<Duration>,
    /// The max timeout that can be selected randomly.
    pub min_election_timeout: usize,
    /// The min timeout that can be selected randomly.
    pub max_election_timeout: usize,
}

impl State {}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            election_time: None,
            election_timeout: None,
            min_election_timeout: 10,
            max_election_timeout: 1000,
        }
    }
}

/// A map of nodes in the cluster shared between a few components in the crate.
// TODO: Refactor into better pattern.
pub type NodeMap = Arc<RwLock<HashMap<NodeId, Node>>>;

/// The primary struct representing the state machine. Contains fields common all roles.
pub struct Raft<S: Role, I: Io, R: Rpc> {
    /// The identifier for this node.
    pub id: NodeId,
    /// The logger implementation for this node.
    pub log: Logger,

    ///
    pub tx: Sender<Command>,

    /// Configuration for this instance.
    pub config: RaftConfig,

    /// A map of known nodes in the cluster.
    pub nodes: NodeMap,

    /// Volatile and persistent state for the state machine. Note that specific additional per-role
    /// state may be contained in that role.
    pub state: State,

    /// The IO implementation used for persisting state.
    pub io: I,

    /// The RPC implementation used for communicating with other nodes.
    pub rpc: R,

    /// An instance containing role specific state and behavior.
    pub role: S,
}

// Base methods for general operations (+ debugging and testing).
impl<S: Role, I: Io, R: Rpc> Raft<S, I, R> {
    /// Add the provided node to our record of the cluster.
    pub fn add_node_to_cluster(&mut self, socket_addr: SocketAddr) {
        info!(self.log, "Adding node"; "address" => format!("{:?}", socket_addr));
    }

    /// Checks the status of the election timer.
    pub fn needs_election(&self) -> bool {
        match (self.state.election_time, self.state.election_timeout) {
            (Some(time), Some(timeout)) => time.elapsed() > timeout,
            _ => false,
        }
    }

    /// Set the current term.
    pub fn term(&mut self, term: u64) {
        self.state.voted_for = None;
        self.state.current_term = term;

        self.role.term(term);
    }
}

/// Since applying command to the state machine can potentially result in any state transition,
/// the result that we get back needs to be general to the possible return types -- easiest
/// way here is just to store the differently sized structs per state in an enum, which will be
/// sized to the largest variant.
pub enum RaftHandle<I: Io, R: Rpc> {
    /// An instance of the state machine in the follower role.
    Follower(Raft<Follower, I, R>),
    /// An instance of the state machine in the candidate role.
    Candidate(Raft<Candidate, I, R>),
    /// An instance of the state machine in the leader role.
    Leader(Raft<Leader, I, R>),
}

impl<I: Io, R: Rpc> RaftHandle<I, R> {
    /// Obtain a new instance of raft initialized in the default follower state.
    pub fn new(config: RaftConfig, tx: Sender<Command>, io: I, rpc: R, logger: Logger, nodes: NodeMap) -> RaftHandle<I, R> {
        let raft = Raft::new(config, tx, io, rpc, Some(logger), Some(nodes));
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

/// Applying a command is the basic way the state machine is moved forward. Each role implements
/// trait to handle how it responds (or does not respond) to particular commands.
pub trait Apply<I: Io, R: Rpc> {
    /// Apply a command to the raft state machine, which may result in a new raft state. Errors
    /// should occur for only truly exceptional conditions, and are provided to allow the wrapping
    /// server containing this state machine to shut down gracefully.
    fn apply(self, command: Command) -> Result<RaftHandle<I, R>, Error>;
}

