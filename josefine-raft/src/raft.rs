use std::{mem, thread};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Error;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::ops::Index;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use slog::Logger;
use tokio::prelude::Future;
use tokio::sync::oneshot;
use tokio::sync::oneshot::channel;

use crate::candidate::Candidate;
use crate::config::RaftConfig;
use crate::follower::Follower;
use crate::leader::Leader;
use actix::{Actor, Handler, Context, Recipient};
use crate::node::RpcMessage;

/// An id that uniquely identifies this instance of Raft.
pub type NodeId = u32;
pub type Term = u64;

pub type LogIndex = u64;

/// Commands that can be applied to the state machine.
#[derive(Debug, Serialize, Deserialize, Clone)]
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
        term: Term,
        /// The id of the candidate requesting the vote.
        candidate_id: NodeId,
        ///
        last_term: Term,
        ///
        last_index: LogIndex,
    },
    /// Respond to a vote from another node.
    VoteResponse {
        /// The term of the voter.
        term: Term,
        /// The id of the voter responding to the vote.
        candidate_id: NodeId,
        /// Whether the vote was granted to the candidate.
        granted: bool,
    },
    /// Request from another node to append entries to our log.
    AppendEntries {
        /// The term of the node requesting entries be appended to our commit log.
        term: Term,
        /// The id of the node sending entries.
        leader_id: NodeId,

        /// The entries to append to our commit log.
        entries: Vec<Entry>,
    },
    AppendResponse {
        node_id: NodeId,
        term: Term,
        index: LogIndex,
    },
    /// Heartbeat from another node.
    Heartbeat {
        /// The term of the node sending a heartbeat.
        term: Term,
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
    fn role(&self) -> RaftRole;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EntryType {
    Entry { data: Vec<u8> },
    Config {},
    Command { command: Command },
}

/// An entry in the commit log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    /// The type of the entry
    pub entry_type: EntryType,
    /// The term of the entry.
    pub term: Term,
    /// The index of the entry within the commit log.
    pub index: LogIndex,
}

/// Contains information about nodes in raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Node {
    /// The id of the node.
    pub id: NodeId,
    /// The socket address for the TCP connection.
    pub addr: SocketAddr,
}

impl Node {}

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
    pub commit_index: LogIndex,
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

/// The primary struct representing the state machine. Contains fields common all roles.
pub struct Raft<T: Role> {
    /// The identifier for this node.
    pub id: NodeId,
    /// The logger implementation for this node.
    pub log: Logger,

    /// Configuration for this instance.
    pub config: RaftConfig,

    /// A map of known nodes in the cluster.
    pub nodes: HashMap<NodeId, Recipient<RpcMessage>>,

    /// Volatile and persistent state for the state machine. Note that specific additional per-role
    /// state may be contained in that role.
    pub state: State,

    /// An instance containing role specific state and behavior.
    pub role: T,
}

// Base methods for general operations (+ debugging and testing).
impl<T: Role> Raft<T> {
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

pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Since applying command to the state machine can potentially result in any state transition,
/// the result that we get back needs to be general to the possible return types -- easiest
/// way here is just to store the differently sized structs per state in an enum, which will be
/// sized to the largest variant.
pub enum RaftHandle {
    /// An instance of the state machine in the follower role.
    Follower(Raft<Follower>),
    /// An instance of the state machine in the candidate role.
    Candidate(Raft<Candidate>),
    /// An instance of the state machine in the leader role.
    Leader(Raft<Leader>),
}

impl RaftHandle {
    /// Obtain a new instance of raft initialized in the default follower state.
    pub fn new(config: RaftConfig, logger: Logger, nodes: HashMap<NodeId, Recipient<RpcMessage>>) -> RaftHandle {
        let raft = Raft::new(config, Some(logger), nodes);
        RaftHandle::Follower(raft.unwrap())
    }
}

impl Apply for RaftHandle {
    fn apply(self, cmd: Command) -> Result<RaftHandle, failure::Error> {
        match self {
            RaftHandle::Follower(raft) => { raft.apply(cmd) }
            RaftHandle::Candidate(raft) => { raft.apply(cmd) }
            RaftHandle::Leader(raft) => { raft.apply(cmd) }
        }
    }
}

/// Applying a command is the basic way the state machine is moved forward. Each role implements
/// trait to handle how it responds (or does not respond) to particular commands.
pub trait Apply {
    /// Apply a command to the raft state machine, which may result in a new raft state. Errors
    /// should occur for only truly exceptional conditions, and are provided to allow the wrapping
    /// server containing this state machine to shut down gracefully.
    fn apply(self, cmd: Command) -> Result<RaftHandle, failure::Error>;
}

struct RaftActor {
    raft: Option<RaftHandle>
}

impl RaftActor {
    pub fn new(config: RaftConfig, logger: Logger, nodes: HashMap<NodeId, Recipient<RpcMessage>>) -> RaftActor {
        RaftActor {
            raft: Some(RaftHandle::new(config, logger, nodes))
        }
    }
}

impl Actor for RaftActor {
    type Context = Context<Self>;
}

impl Handler<RpcMessage> for RaftActor {
    type Result = ();

    fn handle(&mut self, msg: RpcMessage, ctx: &mut Self::Context) -> Self::Result {
        let raft = mem::replace(&mut self.raft, None).unwrap();
        let raft = raft.apply(Command::Ping(1)).unwrap();
        self.raft  = Some(raft);
        ()
    }
}

