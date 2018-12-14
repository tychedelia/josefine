use crate::raft::raft::{Apply, ApplyResult};
use crate::raft::raft::IO;
use crate::raft::raft::Command;
use crate::raft::raft::Role;
use crate::raft::raft::Raft;
use crate::raft::raft::Node;
use crate::raft::config::Config;
use crate::raft::election::Election;
use crate::raft::candidate::Candidate;
use crate::raft::raft::State;
use std::io::Error;
use std::collections::HashMap;
use std::sync::mpsc::{Sender, Receiver, channel};

pub struct Follower {
    pub leader_id: Option<u64>,
}

impl<T: IO> Apply<T> for Raft<Follower, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        match command {
            Command::Append { entries, from, .. } => {
                self.state.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.append(entries);
                Ok(ApplyResult::Follower(self))
            }
            Command::Heartbeat { from, .. } => {
                self.state.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.heartbeat(from);
                Ok(ApplyResult::Follower(self))
            }
            Command::Timeout => {
                let mut raft: Raft<Candidate, T> = Raft::from(self);
                raft.seek_election()
            }
            _ => Ok(ApplyResult::Follower(self))
        }
    }
}

impl<T: IO> Raft<Follower, T> {
    fn new(config: &Config, io: T) -> Result<Raft<Follower, T>, Error> {
        &config.init()?;

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

impl<T: IO> From<Raft<Follower, T>> for Raft<Candidate, T> {
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
             inner: Candidate { election, },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Raft;
    use super::Config;
    use super::IO;
    use super::Apply;
    use super::Command;
    use super::ApplyResult;
    use super::Node;
    use crate::raft::raft::MemoryIO;

    #[test]
    fn follower_to_candidate() {
        let mut follower = Raft::new(&Config { id: 0 }, MemoryIO::new()).unwrap();
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
        let follower = Raft::new(&Config { id: 0 }, MemoryIO::new()).unwrap();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            ApplyResult::Follower(_) => panic!(),
            ApplyResult::Candidate(_) => panic!(),
            ApplyResult::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let follower = Raft::new(&Config { id: 0 }, MemoryIO::new()).unwrap();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            ApplyResult::Follower(follower) => assert_eq!(id, follower.id),
            ApplyResult::Candidate(candidate) => panic!(),
            ApplyResult::Leader(_) => panic!(),
        }
    }
}
