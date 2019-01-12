use std::collections::HashMap;
use std::io::Error;
use std::sync::mpsc::{channel, Receiver, Sender};

use crate::candidate::Candidate;
use crate::config::{Config, ConfigError};
use crate::election::Election;
use crate::raft::{Apply, ApplyResult};
use crate::raft::{Command, NodeId, IO, Node, Raft, Role, State};

pub struct Follower {
    pub leader_id: Option<NodeId>,
}

impl<I: IO> Apply<T> for Raft<Follower, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        match command {
            Command::Append { mut entries, from, .. } => {
                self.state.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.
                    append(&mut entries);
                Ok(ApplyResult::Follower(self))
            }
            Command::Heartbeat { from, .. } => {
                self.state.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.heartbeat(from);
                Ok(ApplyResult::Follower(self))
            }
            Command::Timeout => {
                let raft: Raft<Candidate, T> = Raft::from(self);
                raft.seek_election()
            }
            _ => Ok(ApplyResult::Follower(self))
        }
    }
}

impl<I: IO> Raft<Follower, T> {
    fn new(config: &Config, io: T) -> Result<Raft<Follower, T>, ConfigError> {
        &config.validate()?;

        let (tx, rx): (Sender<Command>, Receiver<Command>) = channel();

        Ok(Raft {
            id: config.id,
            outbox: rx,
            sender: tx,
            state: State::new(),
            cluster: vec![Node::new(config.id)],
            io,
            inner: Follower { leader_id: None },
            role: Role::Follower,
        })
    }
}

impl<I: IO> From<Raft<Follower, T>> for Raft<Candidate, T> {
    fn from(val: Raft<Follower, T>) -> Raft<Candidate, T> {
        let election = Election::new(&val.cluster);
        Raft {
            id: val.id,
            state: val.state,
            outbox: val.outbox,
            sender: val.sender,
            cluster: val.cluster,
            io: val.io,
            role: Role::Candidate,
            inner: Candidate { election },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::MemoryIO;

    use super::Apply;
    use super::ApplyResult;
    use super::Command;
    use super::Config;
    use super::IO;
    use super::Node;
    use super::Raft;

    #[test]
    fn follower_to_candidate() {
        let mut follower = Raft::new(&Config::default(), MemoryIO::new()).unwrap();
        follower.add_node_to_cluster(Node::new(10));

        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            ApplyResult::Follower(_) => panic!(),
            ApplyResult::Candidate(candidate) => {
                assert_eq!(id, candidate.id)
            }
            ApplyResult::Leader(_) => panic!(),
        }
    }

    #[test]
    fn follower_to_leader_single_node_cluster() {
        let follower = Raft::new(&Config::default(), MemoryIO::new()).unwrap();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            ApplyResult::Follower(_) => panic!(),
            ApplyResult::Candidate(_) => panic!(),
            ApplyResult::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let follower = Raft::new(&Config::default(), MemoryIO::new()).unwrap();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            ApplyResult::Follower(follower) => assert_eq!(id, follower.id),
            ApplyResult::Candidate(candidate) => panic!(),
            ApplyResult::Leader(_) => panic!(),
        }
    }
}
