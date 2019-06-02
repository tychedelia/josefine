use std::{mem, thread, fmt};
use std::collections::HashMap;
use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use std::ops::Index;
use std::time::Duration;
use std::time::Instant;
use actix::{Actor, Arbiter, AsyncContext, Context, Handler, Recipient, Supervised, Supervisor, System, SystemRegistry, SystemService, SystemRunner, Message, Addr};
use slog::Logger;
use tokio::prelude::Future;

use crate::candidate::Candidate;
use crate::config::RaftConfig;
use crate::follower::Follower;
use crate::leader::Leader;
use crate::log::get_root_logger;
use crate::node::NodeActor;
use crate::rpc::RpcMessage;
use crate::listener::TcpListenerActor;
use std::fmt::{Debug, Formatter};
use std::error::Error;
use actix::dev::MessageResponse;
use core::borrow::BorrowMut;

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
    Ping(Term, NodeId),
}

/// Shared behavior that all roles of the state machine must implement.
pub trait Role: Debug {
    /// Set the term for the node, reseting the current election.
    fn term(&mut self, term: u64);
    fn role(&self) -> RaftRole;
    fn log(&self) -> &Logger;
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

impl Debug for State {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let timeout = match (self.election_time, self.election_timeout) {
            (Some(time), Some(timeout)) => if timeout > time.elapsed() {
                timeout - time.elapsed()
            } else {
                Duration::from_secs(0)
            },
            _ => Duration::from_secs(0),
        };
        write!(f, "State {{ current_term: {}, voted_for: {:?}, commit_index: {}, last_applied: {}, timeout: {:?} }}",
               self.current_term, self.voted_for, self.commit_index, self.last_applied, timeout)
    }
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
            min_election_timeout: 500,
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
    pub nodes: NodeMap,

    /// Volatile and persistent state for the state machine. Note that specific additional per-role
    /// state may be contained in that role.
    pub state: State,

    /// An instance containing role specific state and behavior.
    pub role: T,
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
            Command::Tick => {}
            Command::Heartbeat { .. } => {}
            _ => debug!(self.role.log(), ""; "role_state" => format!("{:?}", self.role), "state" => format!("{:?}", self.state), "cmd" => format!("{:?}", cmd))
        };
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
    pub fn new(config: RaftConfig, logger: Logger, nodes: NodeMap) -> RaftHandle {
        let raft = Raft::new(config, logger, nodes);
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

pub struct RaftActor {
    raft: Option<RaftHandle>,
    log: Logger,
    config: RaftConfig,
}

impl RaftActor {
    pub fn new(config: RaftConfig, logger: Logger, nodes: NodeMap) -> RaftActor {
        let c = config.clone();
        let log = logger.new(o!());
        RaftActor {
            log: logger,
            raft: Some(RaftHandle::new(c, log, nodes)),
            config,
        }
    }

    fn unwrap(&mut self) -> RaftHandle {
        mem::replace(&mut self.raft, None).expect("Unwrap has been called twice in a row")
    }
}

pub type NodeMap = HashMap<NodeId, Recipient<RpcMessage>>;

pub fn setup<T: Actor<Context=Context<T>> + Send>(log: Logger, config: RaftConfig, actor: Option<T>) {
    let system = System::new("raft");

    if let Some(actor) = actor {
        Arbiter::start(move |_| actor);
    }

    let raft = RaftActor::create(move |ctx| {
        let l = log.new(o!());
        let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), config.port); // TODO: :(
        let raft = ctx.address().recipient();
        let server = Supervisor::start(move |_| TcpListenerActor::new(addr, l, raft));

        let mut nodes = HashMap::new();
        for node in config.clone().nodes {
            let addr = node.addr.clone();
            let log = log.new(o!());
            let raft = ctx.address().recipient();
            let n = Supervisor::start(move |_| NodeActor::new(addr, log, raft));
            nodes.insert(node.id, n.recipient());
        }

        let raft = ctx.address().recipient();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(100));
                raft.try_send(RpcMessage::Tick).unwrap();
            }
        });

        System::current().registry().set(ctx.address());
        RaftActor::new(config, log, nodes)
    });
    system.run();
}

impl Actor for RaftActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(self.log, "Starting Raft"; "config" => format!("{:?}", self.config));
    }
}

impl SystemService for RaftActor {

}

impl Default for RaftActor {
    fn default() -> Self {
        unimplemented!()
    }
}

impl Supervised for RaftActor {

}

impl Handler<RpcMessage> for RaftActor {
    type Result = ();

    fn handle(&mut self, msg: RpcMessage, _ctx: &mut Self::Context) -> Self::Result {
//        info!(self.log, "Received message"; "msg" => format!("{:?}", msg));
        let raft = self.unwrap();
        let raft = raft.apply(msg.into()).unwrap();
        self.raft = Some(raft);
        ()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DebugStateMessage;

    impl Message for DebugStateMessage {
        type Result = Result<State, std::io::Error>;
    }

    impl Handler<DebugStateMessage> for RaftActor {
        type Result = Result<State, std::io::Error>;

        fn handle(&mut self, msg: DebugStateMessage, ctx: &mut Self::Context) -> Self::Result {
            let raft = self.unwrap();
            let state = match &raft {
                RaftHandle::Follower(raft) => Ok(raft.state.clone()),
                RaftHandle::Candidate(raft) => Ok(raft.state.clone()),
                RaftHandle::Leader(raft) => Ok(raft.state.clone()),
            };
            self.raft = Some(raft);
            state
        }
    }


    macro_rules! test_node {
        ($timeout:literal, $test:expr) => {
            struct TestActor;

            impl Actor for TestActor {
                type Context = Context<Self>;

                fn started(&mut self, ctx: &mut Self::Context) {
                    ctx.run_later(Duration::from_secs($timeout), |_act, _ctx| {
                        let raft = System::current().registry().get::<RaftActor>();
                        let state = raft.send(DebugStateMessage)
                            .map(|res| {
                                let state = res.unwrap();
                                $test(state)
                            })
                            .wait();

                        System::current().stop();
                    });
                }
            }

            let log = get_root_logger();
            let raft = setup(log.new(o!()), RaftConfig::default(), Some(TestActor));
        }
    }

    #[test]
    fn it_runs() {
        test_node!(5, |state: State| {
            assert!(state.voted_for.is_some());
            assert_eq!(state.voted_for.unwrap(), 1);
            assert_eq!(state.current_term, 1);
        });
    }
}

