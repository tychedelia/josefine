use std::collections::HashMap;

use crate::raft::Node;
use crate::raft::NodeId;

pub struct Election {
    voter_ids: Vec<NodeId>,
    votes: HashMap<NodeId, bool>,
}

pub enum ElectionStatus {
    Elected,
    Voting,
    Defeated,
}

impl Election {
    pub fn new() -> Election {
        let mut election = Election {
            voter_ids: Vec::new(),
            votes: HashMap::new(),
        };

        election.reset();
        election
    }

    pub fn reset(&mut self) {
        self.voter_ids.clear();
        self.votes.clear();
//
//        for (k, _v) in self.nodes.read().unwrap().iter() {
//            self.voter_ids.push(k.clone());
//        }
    }

    pub fn vote(&mut self, id: NodeId, vote: bool) {
        self.votes.insert(id, vote);
    }

    pub fn election_status(&self) -> ElectionStatus {
        let (votes, total) = self.votes.iter()
            .fold((0, 0), |(mut votes, mut total), (_id, vote)| {
                if *vote {
                    votes += 1;
                }

                total += 1;
                (votes, total)
            });


        if votes > self.quorum_size() {
            ElectionStatus::Elected
        } else if votes - total == self.quorum_size() {
            ElectionStatus::Defeated
        } else {
            ElectionStatus::Voting
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn voters_size(&self) -> usize {
        self.voter_ids.len()
    }

    #[inline]
    fn quorum_size(&self) -> usize {
        if self.voter_ids.len() == 1 {
            return 0;
        }

        (self.voter_ids.len() / 2) + 1
    }
}