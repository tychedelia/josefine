use crate::raft::raft::{Apply, ApplyResult};
use crate::raft::raft::IO;
use crate::raft::raft::Command;
use crate::raft::raft::Role;
use crate::raft::raft::Raft;
use crate::raft::raft::Node;
use crate::raft::candidate::Candidate;
use crate::raft::config::Config;
use std::io::Error;
use std::collections::HashMap;

pub struct Follower {
    pub leader_id: Option<u64>,
}

impl<T: IO> Apply<T> for Raft<Follower, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        match command {
            Command::Append { entries, from, .. } => {
                self.election_time = 0;
                self.inner.leader_id = Some(from);
                self.io.append(entries);
                Ok(ApplyResult::Follower(self))
            }
            Command::Heartbeat { from, .. } => {
                self.election_time = 0;
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

        Ok(Raft {
            id: config.id,
            current_term: 0,
            voted_for: 0,
            commit_index: 0,
            last_applied: 0,
            election_time: 0,
            heartbeat_time: 0,
            election_timeout: 0,
            heartbeat_timeout: 0,
            min_election_timeout: 0,
            max_election_timeout: 0,
            cluster: vec![Node::new(config.id)],
            io,
            inner: Follower { leader_id: None },
            role: Role::Follower,
        })
    }
}

impl<T: IO> From<Raft<Follower, T>> for Raft<Candidate, T> {
    fn from(val: Raft<Follower, T>) -> Raft<Candidate, T> {
         Raft {
            id: val.id,
            current_term: val.current_term + 1,
            voted_for: val.id, // vote for self
            commit_index: val.commit_index,
            last_applied: val.last_applied,
            election_time: val.election_time,
            heartbeat_time: val.heartbeat_time,
            election_timeout: val.election_timeout,
            heartbeat_timeout: val.heartbeat_timeout,
            min_election_timeout: val.min_election_timeout,
            max_election_timeout: val.heartbeat_timeout,
             cluster: val.cluster,
             io: val.io,
            role: Role::Candidate,
            inner: Candidate { votes: HashMap::new() },
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
        follower.add_node_to_cluster(Node { id: 100 });

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
