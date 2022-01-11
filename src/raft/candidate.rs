use std::time::Instant;

use anyhow::{Error, Result};

use crate::raft::election::{Election, ElectionStatus};
use crate::raft::follower::Follower;
use crate::raft::leader::Leader;
use crate::raft::progress::ReplicationProgress;

use crate::raft::rpc::Address;
use crate::raft::{Apply, ClientRequest, RaftHandle, RaftRole, Term};
use crate::raft::{Command, NodeId};
use crate::raft::{Raft, Role};
use std::collections::HashSet;
use crate::raft::chain::BlockId;

#[derive(Debug)]
pub struct Candidate {
    pub election: Election,
    pub queued_reqs: Vec<ClientRequest>,
}

impl Raft<Candidate> {
    pub(crate) fn seek_election(mut self) -> Result<RaftHandle> {
        self.state.voted_for = Some(self.id);
        self.state.current_term += 1;
        let from = self.id;
        let term = self.state.current_term;

        for _node in &self.config.nodes {
            self.send_all(Command::VoteRequest {
                term,
                candidate_id: from,
                last_term: term,
                head: self.chain.get_head(),
            })?;
        }

        // Vote for self,
        self.apply(Command::VoteResponse {
            from,
            term,
            granted: true,
        })
    }

    #[tracing::instrument]
    fn apply_tick(mut self) -> Result<RaftHandle, Error> {
        if self.needs_election() {
            return match self.role.election.election_status() {
                ElectionStatus::Voting => {
                    tracing::info!("continue voting");
                    self.state.voted_for = None;
                    let raft: Raft<Follower> = Raft::from(self);
                    Ok(raft.apply(Command::Timeout)?)
                }
                ElectionStatus::Defeated => {
                    tracing::info!("defeat");
                    self.state.voted_for = None;
                    let raft: Raft<Follower> = Raft::from(self);
                    Ok(raft.apply(Command::Timeout)?)
                }
                _ => panic!("this should never happen"),
            };
        }

        Ok(RaftHandle::Candidate(self))
    }

    #[tracing::instrument]
    fn apply_vote_request(mut self, candidate_id: NodeId, term: Term) -> Result<RaftHandle, Error> {
        if term > self.state.current_term {
            tracing::trace!("become follower");
            self.term(term);
            return Ok(RaftHandle::Follower(Raft::from(self)));
        }

        self.send(
            Address::Peer(candidate_id),
            Command::VoteResponse {
                from: self.id,
                term: self.state.current_term,
                granted: false,
            },
        )?;

        Ok(RaftHandle::Candidate(self))
    }

    #[tracing::instrument]
    fn apply_vote_response(mut self, granted: bool, from: NodeId) -> Result<RaftHandle, Error> {
        self.role.election.vote(from, granted);
        match self.role.election.election_status() {
            ElectionStatus::Elected => {
                tracing::info!("elect");
                let raft = Raft::from(self);
                raft.heartbeat()?;
                Ok(RaftHandle::Leader(raft))
            }
            ElectionStatus::Voting => Ok(RaftHandle::Candidate(self)),
            ElectionStatus::Defeated => {
                tracing::info!("defeat");
                self.state.voted_for = None;
                Ok(RaftHandle::Follower(Raft::from(self)))
            }
        }
    }

    #[tracing::instrument]
    fn apply_append_entries(mut self, term: Term) -> Result<RaftHandle, Error> {
        // While waiting for votes, a candidate may receive an
        // AppendEntries RPC from another server claiming to be
        // leader. If the leader’s term (included in its RPC) is at least
        // as large as the candidate’s current term, then the candidate
        // recognizes the leader as legitimate and returns to follower
        // state.
        if term >= self.state.current_term {
            tracing::trace!("");
            let raft: Raft<Follower> = Raft::from(self);
            //                    raft.io.append(entries)?;
            return Ok(RaftHandle::Follower(raft));
        }

        // TODO: If the term in the RPC is smaller than the candidate’s
        // current term, then the candidate rejects the RPC and continues in candidate state.

        Ok(RaftHandle::Candidate(self))
    }

    #[tracing::instrument]
    fn apply_heartbeat(mut self, term: Term, leader_id: NodeId, commit: BlockId) -> Result<RaftHandle, Error> {
            tracing::trace!("receive higher term");
            let has_committed = self.chain.has(&commit)?;
            let commit = self.chain.get_commit();
            self.term(term);
            self.state.voted_for = Some(leader_id);
            let raft: Raft<Follower> = Raft::from(self);
            raft.send(
                Address::Peer(leader_id),
                Command::HeartbeatResponse {
                    commit,
                    has_committed,
                },
            )?;
            return Ok(RaftHandle::Follower(raft));
        }
}

impl Role for Candidate {
    fn term(&mut self, _term: u64) {
        self.election.reset();
    }

    fn role(&self) -> RaftRole {
        RaftRole::Candidate
    }
}

impl Apply for Raft<Candidate> {
    #[tracing::instrument]
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);

        match cmd {
            Command::Tick => {
                self.apply_tick()
            }
            Command::VoteRequest {
                candidate_id, term, ..
            } => {
                self.apply_vote_request(candidate_id, term)
            }
            Command::VoteResponse { granted, from, .. } => {
                self.apply_vote_response(granted, from)
            }
            Command::AppendEntries {
                blocks: _, term, ..
            } => {
                self.apply_append_entries(term)
            }
            Command::Heartbeat {
                term, leader_id, commit
            } => {
                self.apply_heartbeat(term, leader_id, commit)
            }
            Command::ClientRequest(req) => {
                self.role.queued_reqs.push(req);
                Ok(RaftHandle::Candidate(self))
            }
            _ => Ok(RaftHandle::Candidate(self)),
        }
    }
}

impl From<Raft<Candidate>> for Raft<Follower> {
    fn from(val: Raft<Candidate>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            role: Follower {
                leader_id: None,
                proxied_reqs: HashSet::new(),
                queued_reqs: val.role.queued_reqs,
            },
            config: val.config,
            chain: val.chain,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

impl From<Raft<Candidate>> for Raft<Leader> {
    fn from(val: Raft<Candidate>) -> Raft<Leader> {
        let mut nodes: Vec<NodeId> = val.config.nodes.iter().map(|x| x.id).collect();
        nodes.push(val.id);
        let progress = ReplicationProgress::new(nodes);
        let leader = Raft {
            id: val.id,
            state: val.state,
            role: Leader {
                progress,
                heartbeat_time: Instant::now(),
                heartbeat_timeout: val.config.heartbeat_timeout,
            },
            config: val.config,
            chain: val.chain,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        };

        // run any transition specific logic
        leader.on_transition().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::chain::BlockId;
    use crate::raft::Command;
    use crate::raft::test::new_candidate;

    #[tokio::test]
    async fn apply_heartbeat() -> anyhow::Result<()> {
        let ((mut rpc_rx, _), candidate) = new_candidate();
        let follower = candidate
            .apply_heartbeat(11, 6, BlockId::new(1))?
            .get_follower()
            .unwrap();
        // we voted for the leader
        assert!(follower.state.voted_for.is_some());
        assert_eq!(follower.state.voted_for.unwrap(), 6);
        assert_eq!(follower.state.current_term, 11);
        let msg = rpc_rx.recv().await.unwrap();
        // but we don't have block 1 in our chain
        assert_eq!(
            msg.command,
            Command::HeartbeatResponse {
                commit: BlockId::new(0),
                has_committed: false
            }
        );
        Ok(())
    }
}
