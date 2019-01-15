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

pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub log: Logger,
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Follower, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, io::Error> {
        match command {
            Command::Append { mut entries, from, .. } => {
                self.state.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.
                    append(&mut entries);
                Ok(RaftHandle::Follower(self))
            }
            Command::Heartbeat { from, .. } => {
                self.state.election_time = 0;
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

        let raft = Raft {
            id: config.id,
            state: State::new(),
            nodes,
            io,
            rpc,
            inner: Follower { leader_id: None, log: log.new(o!("role" => "follower")) },
            role: Role::Follower,
            log,
        };

        Ok(raft)
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
    use crate::rpc::NoopRpc;

    use super::Apply;
    use super::Command;
    use super::Io;
    use super::Node;
    use super::Raft;
    use super::RaftConfig;
    use super::RaftHandle;
    use std::rc::Rc;
    use std::cell::RefCell;

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
