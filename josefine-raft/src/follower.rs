use std::collections::HashMap;
use std::io::Error;
use std::sync::mpsc::{channel, Receiver, Sender};

use crate::candidate::Candidate;
use crate::config::{Config, ConfigError};
use crate::election::Election;
use crate::raft::{Apply, RaftHandle};
use crate::raft::{Command, NodeId, Io, Node, Raft, Role, State};
use crate::rpc::Rpc;

pub struct Follower {
    pub leader_id: Option<NodeId>,
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Follower, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, Error> {
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
            Command::Timeout => {
                let raft: Raft<Candidate, I, R> = Raft::from(self);
                raft.seek_election()
            }
            _ => Ok(RaftHandle::Follower(self))
        }
    }
}

impl<I: Io, R: Rpc> Raft<Follower, I, R> {
    fn new(config: &Config, io: I, rpc: R) -> Result<Raft<Follower, I, R>, ConfigError> {
        &config.validate()?;

        Ok(Raft {
            id: config.id,
            state: State::new(),
            cluster: vec![Node::new(config.id)],
            io,
            rpc,
            inner: Follower { leader_id: None },
            role: Role::Follower,
        })
    }
}

impl<I: Io, R: Rpc> From<Raft<Follower, I, R>> for Raft<Candidate, I, R> {
    fn from(val: Raft<Follower, I, R>) -> Raft<Candidate, I, R> {
        let election = Election::new(&val.cluster);
        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            rpc: val.rpc,
            role: Role::Candidate,
            inner: Candidate { election },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::MemoryIo;

    use super::Apply;
    use super::RaftHandle;
    use super::Command;
    use super::Config;
    use super::Io;
    use super::Node;
    use super::Raft;
    use crate::rpc::MemoryRpc;
    use std::sync::mpsc::channel;

    #[test]
    fn follower_to_candidate() {
        let (tx, rx) = channel();
        let mut follower = Raft::new(&Config::default(), MemoryIo::new(), MemoryRpc::new(Config::default(), tx)).unwrap();
        follower.add_node_to_cluster(Node::new(10));

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
        let (tx, rx) = channel();
        let follower = Raft::new(&Config::default(), MemoryIo::new(), MemoryRpc::new(Config::default(), tx)).unwrap();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let (tx, rx) = channel();
        let follower = Raft::new(&Config::default(), MemoryIo::new(), MemoryRpc::new(Config::default(), tx)).unwrap();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(candidate) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }
}
