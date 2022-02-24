//! # Josefine Raft
//!
//! This implementation of the [Raft](raft.github.io) consensus algorithm uses a modified form
//! called [Chained Raft](https://decentralizedthoughts.github.io/2021-07-17-simplifying-raft-with-chaining/),
//! which models distributed state as an immutable directed acyclic graph rather than the
//! commit log described in the original Raft paper.
//!
//! The primary benefit of using a DAG rather than log is that it does not require complicated
//! behavior on leader election to guarantee the the log of all the replicas are up to date,
//! potentially rewriting existing entries at a given index with new entries from the a higher
//! turn provided by a new leader. By using an immutable graph of "blocks" on a chain, appending
//! never requires updating existing state, but only appending new nodes to the graph. In this way,
//! rather than being overwritten in a mutable log, existing uncommitted entries are left as
//! vestiges on the data structure.
//!
//! ```text
//!                              stale
//!                               |
//! [G] -> [A] -> [B] -> [C]  -> [D]            head
//!                       |    \                 |
//!                     commit  \ [E] -> [F] -> [G]
//!
//! ```
//!
//! Rather than indexing into the log, each cluster member tracks a "pointer" to the current head
//! of the chain and the guaranteed stable commit. Because individual blocks do not need to be
//! rewritten, the term of a block is tracked only for determining the last appended term.
//!
//! ## Storage
//!
//! Although presented at a high level as a graph or "chain", with "pointers" to each node,
//! this is a higher level abstraction of the distributed state and does not represent how state
//! is represented in the underlying storage engine.
//!
//! Because we are implementing for a Kafka-like system, we can make a number of assumptions
//! about storage that may not apply to other workloads using raft.

use std::fmt;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use rpc::Response;

use crate::raft::chain::{Block, BlockId, Chain};
use crate::raft::config::RaftConfig;
use crate::raft::follower::Follower;
use crate::raft::fsm::Instruction;
use crate::raft::leader::Leader;
use crate::raft::rpc::{Address, Message, ResponseError};
use crate::raft::server::{Server, ServerRunOpts};
use crate::raft::{candidate::Candidate, rpc::Proposal};
use crate::Shutdown;
use anyhow::Result;

use uuid::Uuid;

mod candidate;
mod chain;
pub mod client;
pub mod config;
mod election;
mod follower;
pub mod fsm;
mod leader;
mod progress;
pub mod rpc;
mod server;
mod tcp;
mod test;

#[derive(Debug)]
pub struct JosefineRaft {
    server: Server,
}

impl JosefineRaft {
    pub fn new(config: config::RaftConfig) -> Self {
        JosefineRaft {
            server: Server::new(config),
        }
    }

    #[tracing::instrument]
    pub async fn run<T: 'static + fsm::Fsm>(
        self,
        fsm: T,
        client_rx: UnboundedReceiver<(
            Proposal,
            oneshot::Sender<std::result::Result<Response, ResponseError>>,
        )>,
        shutdown: Shutdown,
    ) -> Result<RaftHandle> {
        self.server
            .run(ServerRunOpts {
                fsm,
                client_rx,
                shutdown,
            })
            .await
    }

    #[tracing::instrument]
    pub async fn run_for<T: 'static + fsm::Fsm>(
        self,
        duration: Duration,
        fsm: T,
        client_rx: UnboundedReceiver<(
            Proposal,
            oneshot::Sender<std::result::Result<Response, ResponseError>>,
        )>,
        shutdown: Shutdown,
    ) -> Result<RaftHandle> {
        let s = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration);
            s.shutdown();
        });

        self.server
            .run(ServerRunOpts {
                fsm,
                client_rx,
                shutdown,
            })
            .await
    }
}

/// A unique id that uniquely identifies an instance of Raft.
pub type NodeId = u32;

/// A term serves as a logical clock that increases monotonically when a new election begins.
pub type Term = u64;
/// Each entry has an index in the log, which with the term, describes the unique position of an entry in the log.
pub type LogIndex = u64;

pub type ClientRequestId = Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientRequest {
    id: ClientRequestId,
    address: Address,
    proposal: Proposal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientResponse {
    id: ClientRequestId,
    res: std::result::Result<Response, ResponseError>,
}

/// Commands that can be applied to the state machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Move the state machine forward.
    Tick,
    /// Propose a change
    Propose,
    /// Request that this instance vote for the provide node.
    VoteRequest {
        /// The term of the candidate.
        term: Term,
        /// The id of the candidate requesting the vote.
        candidate_id: NodeId,
        /// Term of the last log entry.
        last_term: Term,
        head: BlockId,
    },
    /// Respond to a vote from another node.
    VoteResponse {
        /// The term of the voter.
        term: Term,
        /// The id of the voter responding to the vote.
        from: NodeId,
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
        blocks: Vec<Block>,
    },
    AppendResponse {
        /// The id of the responding node.
        node_id: NodeId,
        /// The term of the responding node.
        term: Term,
        /// The responding node's current index.
        head: BlockId,
        /// Whether the entries were successfully applied.
        success: bool,
    },
    /// Heartbeat from another node.
    Heartbeat {
        /// The term of the node sending a heartbeat.
        term: Term,
        /// The commited index
        commit: BlockId,
        /// The id of the node sending a heartbeat.
        leader_id: NodeId,
    },
    HeartbeatResponse {
        /// The leader's commit index
        commit: BlockId,
        /// Whether this node needs replication of committed entries
        has_committed: bool,
    },
    /// Timeout on an event (i.e. election).
    Timeout,
    /// Don't do anything.
    Noop,
    // Service a client request
    ClientRequest(ClientRequest),
    // Respond to a client.
    // this is a bit weird, since this isn't ever applied to a raft node, but received and proxied by the server event loop
    ClientResponse(ClientResponse),
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Shared behavior that all roles of the state machine must implement.
pub trait Role: Debug {
    /// Set the term for the node, reseting the current election.
    fn term(&mut self, term: Term);
    fn role(&self) -> RaftRole;
}

#[derive(Serialize, PartialEq, Deserialize, Debug, Clone)]
pub enum EntryType {
    Entry { data: Vec<u8> },
    Control,
}

/// An entry in the commit log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    /// The time the election was started.
    pub election_time: Option<Instant>,
    /// The timeout for the current election.
    pub election_timeout: Option<Duration>,
    /// The max timeout that can be selected randomly.
    pub min_election_timeout: usize,
    /// The min timeout that can be selected randomly.
    pub max_election_timeout: usize,
}

impl Debug for State {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let timeout = match (self.election_time, self.election_timeout) {
            (Some(time), Some(timeout)) => {
                if timeout > time.elapsed() {
                    timeout - time.elapsed()
                } else {
                    Duration::from_secs(0)
                }
            }
            _ => Duration::from_secs(0),
        };
        write!(
            f,
            "State {{ current_term: {}, voted_for: {:?}, timeout: {:?} }}",
            self.current_term, self.voted_for, timeout
        )
    }
}

impl State {}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            election_time: None,
            election_timeout: None,
            min_election_timeout: 500,
            max_election_timeout: 1000,
        }
    }
}

/// The primary struct representing the state machine. Contains fields common all roles.

pub struct Raft<T: Role + Debug> {
    /// The identifier for this node.
    pub id: NodeId,
    /// Configuration for this instance.
    pub config: RaftConfig,
    /// Volatile and persistent state for the state machine. Note that specific additional per-role
    /// state may be contained in that role.
    pub state: State,
    /// An instance containing role specific state and behavior.
    pub role: T,
    pub chain: Chain,
    /// Channel to send messages to other nodes.
    pub rpc_tx: UnboundedSender<Message>,
    /// Channel to send entries to fsm driver.
    pub fsm_tx: UnboundedSender<Instruction>,
}

impl<T: Role + Debug> Debug for Raft<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Raft {{ id: {}, state: {:?} }}", self.id, self.state)
    }
}

// Base methods for general operations (+ debugging and testing).
impl<T: Role> Raft<T> {
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

    pub fn log_command(&self, cmd: &Command) {
        match cmd {
            cmd @ Command::Tick => {
                tracing::trace!(%cmd, "command")
            }
            cmd @ Command::Heartbeat { .. } => {
                tracing::trace!(%cmd, "command")
            }
            cmd @ Command::HeartbeatResponse { .. } => {
                tracing::trace!(%cmd, "command")
            }
            cmd @ Command::AppendEntries { .. } => {
                tracing::trace!(%cmd, "command")
            }
            cmd @ Command::AppendResponse { .. } => {
                tracing::trace!(%cmd, "command")
            }
            _ => {
                tracing::info!(%cmd, "command");
            }
        };
    }

    pub fn send(&self, to: Address, cmd: Command) -> Result<()> {
        let msg = Message::new(Address::Peer(self.id), to, cmd);
        self.rpc_tx.send(msg)?;
        Ok(())
    }

    pub fn send_all(&self, cmd: Command) -> Result<()> {
        let msg = Message::new(Address::Peer(self.id), Address::Peers, cmd);
        self.rpc_tx.send(msg)?;
        Ok(())
    }
}

pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Handle to some variant of the state machine. Commands should always be dispatched to the
/// state machine via [`Apply`]. The concrete variant of the state machine should not be matched
/// on directly, as state transitions are handled entirely .
// Since applying command to the state machine can potentially result in any state transition,
// the result that we get back needs to be general to the possible return types -- easiest
// way here is just to store the differently sized structs per state in an enum, which will be
// sized to the largest variant.
#[derive(Debug)]
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
    pub fn new(
        config: RaftConfig,
        rpc_tx: UnboundedSender<Message>,
        fsm_tx: UnboundedSender<Instruction>,
    ) -> RaftHandle {
        let raft = Raft::new(config, rpc_tx, fsm_tx);
        RaftHandle::Follower(raft.unwrap())
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, Self::Follower(_))
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Self::Candidate(_))
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader(_))
    }

    pub fn get_follower(self) -> Option<Raft<Follower>> {
        match self {
            RaftHandle::Follower(r) => Some(r),
            _ => None,
        }
    }

    pub fn get_candidate(self) -> Option<Raft<Candidate>> {
        match self {
            RaftHandle::Candidate(r) => Some(r),
            _ => None,
        }
    }

    pub fn get_leader(self) -> Option<Raft<Leader>> {
        match self {
            RaftHandle::Leader(r) => Some(r),
            _ => None,
        }
    }
}

impl Apply for RaftHandle {
    fn apply(self, cmd: Command) -> Result<RaftHandle> {
        match self {
            RaftHandle::Follower(raft) => raft.apply(cmd),
            RaftHandle::Candidate(raft) => raft.apply(cmd),
            RaftHandle::Leader(raft) => raft.apply(cmd),
        }
    }
}

/// Applying a command is the basic way the state machine is moved forward. Each role implements
/// trait to handle how it responds (or does not respond) to particular commands.
pub trait Apply {
    /// Apply a command to the raft state machine, which may result in a new raft state. Errors
    /// should occur for only truly exceptional conditions, and are provided to allow the wrapping
    /// server containing this state machine to shut down gracefully.

    fn apply(self, cmd: Command) -> Result<RaftHandle>;
}

#[cfg(test)]
mod tests {
    use crate::raft::chain::Chain;
    use crate::raft::rpc::Address;
    use crate::raft::{Command, Raft, RaftRole, Role, Term};
    use std::time::Instant;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct RaftT {
        inner: u8,
    }

    impl Role for RaftT {
        fn term(&mut self, term: Term) {
            self.inner = term as u8;
        }

        fn role(&self) -> RaftRole {
            unimplemented!()
        }
    }

    #[test]
    fn need_election() {
        let (fsm_tx, _fsm_rx) = tokio::sync::mpsc::unbounded_channel();
        let (rpc_tx, _rpc_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut raft = Raft {
            id: 0,
            config: Default::default(),
            state: Default::default(),
            role: RaftT { inner: 0 },
            chain: Chain::new(tempdir().unwrap()).unwrap(),
            rpc_tx,
            fsm_tx,
        };
        raft.state.election_time = Some(Instant::now());
        raft.state.election_timeout = Some(raft.config.election_timeout);
        std::thread::sleep(raft.config.election_timeout);
        assert!(raft.needs_election());
    }

    #[tokio::test]
    async fn send_all() -> anyhow::Result<()> {
        let (fsm_tx, _fsm_rx) = tokio::sync::mpsc::unbounded_channel();
        let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft = Raft {
            id: 0,
            config: Default::default(),
            state: Default::default(),
            role: RaftT { inner: 0 },
            chain: Chain::new(tempdir()?)?,
            rpc_tx,
            fsm_tx,
        };
        raft.send_all(Command::Noop)?;
        let msg = rpc_rx.recv().await.unwrap();
        assert_eq!(msg.from, Address::Peer(raft.id));
        assert_eq!(msg.to, Address::Peers);
        assert_eq!(msg.command, Command::Noop);
        Ok(())
    }

    #[test]
    fn term() {
        let (fsm_tx, _fsm_rx) = tokio::sync::mpsc::unbounded_channel();
        let (rpc_tx, _rpc_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut raft = Raft {
            id: 0,
            config: Default::default(),
            state: Default::default(),
            role: RaftT { inner: 0 },
            chain: Chain::new(tempdir().unwrap()).unwrap(),
            rpc_tx,
            fsm_tx,
        };
        raft.term(11);
        assert_eq!(raft.role.inner, 11);
    }
}
