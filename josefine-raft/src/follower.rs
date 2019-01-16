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
use crate::raft::RaftRole;

pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub log: Logger,
}

impl RaftRole for Follower {
    fn term(&mut self, term: u64) {
        self.leader_id = None;
    }
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Follower, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, io::Error> {
        match command {
            Command::Tick => {
                info!(self.inner.log, "Tick!");
                if self.has_timed_out() {
                    return self.apply(Command::Timeout);
                }

                Ok(RaftHandle::Follower(self))
            }
            Command::Append { mut entries, from, .. } => {
                self.state.election_time = Some(Instant::now());
                self.inner.leader_id = Some(from);
                self.io.append(&mut entries);
                Ok(RaftHandle::Follower(self))
            }
            Command::Heartbeat { from, .. } => {
                self.state.election_time = Some(Instant::now());
                self.inner.leader_id = Some(from);
                self.io.heartbeat(from);
                Ok(RaftHandle::Follower(self))
            }
            Command::VoteRequest { from, .. } => {
                self.rpc.respond_vote(&self.state, from, true);
                Ok(RaftHandle::Follower(self))
            }
            Command::Timeout => {
                let raft: Raft<Candidate, I, R> = Raft::from(self);
                raft.seek_election()
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
    pub fn new(config: RaftConfig, io: I, rpc: R, logger: Option<Logger>, nodes: Option<NodeMap>) -> Result<Raft<Follower, I, R>, ConfigError> {
        config.validate()?;

        let log = match logger {
            None => get_logger().new(o!("id" => config.id)),
            Some(logger) => logger.new(o!("id" => config.id)),
        };

        let nodes = match nodes {
            Some(nodes) => nodes,
            None => Rc::new(RefCell::new(HashMap::new())),
        };

        nodes.borrow_mut().insert(config.id, Node {
            id: config.id,
            ip: config.ip,
            port: config.port,
        });

        let mut raft = Raft {
            id: config.id,
            state: State::new(),
            nodes,
            io,
            rpc,
            inner: Follower {
                leader_id: None,
                log: log.new(o!("role" => "follower")),
            },
            role: Role::Follower,
            log,
        };

        raft.init();
        Ok(raft)
    }

    fn init(&mut self) {
        self.set_election_timeout();
    }

    fn has_timed_out(&self) -> bool {
        match (self.state.voted_for, self.state.election_time, self.state.election_timeout) {
            (None, Some(time), Some(timeout)) => time.elapsed() > timeout,
            _ => false,
        }
    }

    fn get_randomized_timeout(&self) -> Duration {
        let prev_timeout = self.state.election_timeout;
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
            role: Role::Candidate,
            inner: Candidate { election, log: val.log.new(o!("role" => "candidate")) },
            log: val.log,
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

    #[test]
    fn follower_to_candidate() {
        let mut follower = new_follower();
        follower.add_node_to_cluster(Node::new(10, IpAddr::from([0, 0, 0, 0]), 0));

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
