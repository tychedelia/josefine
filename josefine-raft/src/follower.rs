use std::collections::HashMap;
use std::io;

use slog;
use slog::Drain;
use slog::Logger;

use crate::candidate::Candidate;
use crate::config::{ConfigError, RaftConfig};
use crate::election::Election;
use crate::raft::{Apply, RaftHandle};
use crate::raft::{Command, Io, Node, NodeId, Raft, Role, State};
use crate::raft::NodeMap;
use crate::rpc::Rpc;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use rand::Rng;
use threadpool::ThreadPool;
use std::time::Instant;
use std::sync::RwLock;
use std::net::SocketAddr;

pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub log: Logger,
}

impl Role for Follower {
    fn term(&mut self, _term: u64) {
        self.leader_id = None;
    }
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Follower, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, io::Error> {
        match command {
            Command::Start => {
                for addr in &self.config.nodes {
                    self.rpc.add_self_to_cluster(addr);
                }

                Ok(RaftHandle::Follower(self))
            }
            Command::Tick => {
                if self.has_timed_out() {
                    return self.apply(Command::Timeout);
                }

                Ok(RaftHandle::Follower(self))
            }
            Command::AppendEntries { mut entries, leader_id, .. } => {
                self.state.election_time = Some(Instant::now());
                self.role.leader_id = Some(leader_id);
                self.state.voted_for = Some(leader_id);
                self.io.append(&mut entries);
                Ok(RaftHandle::Follower(self))
            }
            Command::Heartbeat {leader_id, .. } => {
                self.state.election_time = Some(Instant::now());
                self.role.leader_id = Some(leader_id);
                self.io.heartbeat(leader_id);
                Ok(RaftHandle::Follower(self))
            }
            Command::VoteRequest { candidate_id, last_index, last_term, .. } => {
                if self.state.current_term > last_term {
                    self.rpc.respond_vote(&self.state, candidate_id,false);
                }
                else if self.state.commit_index > last_index {
                    self.rpc.respond_vote(&self.state, candidate_id,false);
                }
                else {
                    self.rpc.respond_vote(&self.state, candidate_id, true);
                }
                Ok(RaftHandle::Follower(self))
            }
            Command::Timeout => {
                if self.state.voted_for.is_none() {
                    let raft: Raft<Candidate, I, R> = Raft::from(self);
                    return raft.seek_election();
                }
                Ok(RaftHandle::Follower(self))
            }
            Command::Ping(node_id) => {
                self.rpc.ping(node_id);
                Ok(RaftHandle::Follower(self))
            }
            Command::AddNode(node) => {
                self.add_node_to_cluster(node);
                Ok(RaftHandle::Follower(self))
            }
            _ => Ok(RaftHandle::Follower(self))
        }
    }
}

impl<I: Io, R: Rpc> Raft<Follower, I, R> {
    /// Creates an initialized instance of Raft in the follower with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration to use for creating the state machine.
    /// * `io` - The implementation used to persist the non-volatile state of the state machine and
    /// entries for the commit log.
    /// * `logger` - An optional logger implementation.
    /// * `nodes` - An optional map of nodes present in the cluster.
    ///
    /// # Example
    ///
    /// ```
    /// let raft = Raft::new(io, rpc, logger, nodes);
    /// ```
    ///
    pub fn new(config: RaftConfig, io: I, rpc: R, logger: Option<Logger>, nodes: Option<NodeMap>)
               -> Result<Raft<Follower, I, R>, ConfigError> {
        config.validate()?;

        let log = match logger {
            None => get_logger().new(o!("id" => config.id)),
            Some(logger) => logger.new(o!("id" => config.id)),
        };

        let nodes = match nodes {
            Some(nodes) => nodes,
            None => Arc::new(RwLock::new(HashMap::new())),
        };

        nodes.write().unwrap().insert(config.id, Node {
            id: config.id,
            addr: SocketAddr::new(config.ip, config.port),
        });

        let mut raft = Raft {
            id: config.id,
            config,
            state: State::default(),
            nodes,
            io,
            rpc,
            role: Follower {
                leader_id: None,
                log: log.new(o!("role" => "follower")),
            },
            log,
        };

        raft.init();
        Ok(raft)
    }

    fn init(&mut self) {
        self.set_election_timeout();
    }

    fn has_timed_out(&self) -> bool {
        match (self.state.election_time, self.state.election_timeout) {
            (Some(time), Some(timeout)) => time.elapsed() > timeout,
            _ => false,
        }
    }

    fn get_randomized_timeout(&self) -> Duration {
        let _prev_timeout = self.state.election_timeout;
        let timeout = rand::thread_rng().gen_range(self.state.min_election_timeout, self.state.max_election_timeout);
        Duration::from_millis(timeout as u64)
    }

    fn set_election_timeout(&mut self) {
        self.state.election_timeout = Some(self.get_randomized_timeout());
        self.state.election_time = Some(Instant::now());
    }
}

fn get_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    Logger::root(drain, o!())
}

impl<I: Io, R: Rpc> From<Raft<Follower, I, R>> for Raft<Candidate, I, R> {
    fn from(val: Raft<Follower, I, R>) -> Raft<Candidate, I, R> {
        let election = Election::new(val.nodes.clone());
        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            io: val.io,
            rpc: val.rpc,
            role: Candidate { election, log: val.log.new(o!("role" => "candidate")) },
            log: val.log,
            config: val.config,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use crate::follower::Follower;
    use crate::raft::MemoryIo;

    use super::Apply;
    use super::Command;
    use super::Io;
    use super::Node;
    use super::Raft;
    use super::RaftConfig;
    use super::RaftHandle;
    use std::rc::Rc;
    use std::cell::RefCell;
    use crate::rpc::NoopRpc;
    use std::net::SocketAddr;

    #[test]
    fn follower_to_candidate() {
        let mut follower = new_follower();
        follower.add_node_to_cluster(Node { id: 10, addr: SocketAddr::new("127.0.0.1".parse().unwrap(), 8080) });

        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(candidate) => {
                assert_eq!(id, candidate.id)
            }
            RaftHandle::Leader(_) => panic!(),
        }
    }

    #[test]
    fn follower_to_leader_single_node_cluster() {
        let follower = new_follower();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let follower = new_follower();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }

    fn new_follower() -> Raft<Follower, MemoryIo, NoopRpc> {
        let config = RaftConfig::default();
        Raft::new(config, MemoryIo::new(), NoopRpc::new(), None, None).unwrap()
    }
}
