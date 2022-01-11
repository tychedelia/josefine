use std::time::Duration;
use std::time::Instant;

use rand::Rng;

use crate::raft::candidate::Candidate;
use crate::raft::chain::{Block, BlockId, Chain};
use crate::raft::election::Election;
use crate::raft::fsm::Instruction;
use crate::raft::rpc::{Address, Message, Proposal, Response, ResponseError};
use crate::raft::Command::VoteResponse;
use crate::raft::{Apply, ClientRequest, ClientResponse, RaftHandle, RaftRole, Term};
use crate::raft::{ClientRequestId, RaftConfig};
use crate::raft::{Command, NodeId, Raft, Role, State};
use anyhow::Result;
use std::collections::HashSet;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub proxied_reqs: HashSet<ClientRequestId>,
    pub queued_reqs: Vec<ClientRequest>,
}

impl Role for Follower {
    fn term(&mut self, _term: u64) {
        self.leader_id = None;
    }

    fn role(&self) -> RaftRole {
        RaftRole::Follower
    }
}

impl Apply for Raft<Follower> {
    #[tracing::instrument]
    fn apply(self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => self.apply_tick(),
            Command::AppendEntries {
                blocks,
                leader_id,
                term,
            } => self.apply_append_entries(blocks, leader_id, term),
            Command::Heartbeat {
                leader_id,
                term,
                commit,
            } => self.apply_heartbeat(leader_id, term, commit),
            Command::VoteRequest {
                candidate_id,
                last_term,
                head,
                ..
            } => self.apply_vote_request(candidate_id, last_term, head),
            Command::Timeout => self.apply_timeout(),
            Command::ClientRequest(req) => self.apply_client_request(req),
            Command::ClientResponse(res) => self.apply_client_response(res.id, res.res),
            _ => self.apply_self(),
        }
    }
}

impl Raft<Follower> {
    /// Creates an initialized instance of Raft in the follower with the provided configuration.
    pub fn new(
        config: RaftConfig,
        rpc_tx: UnboundedSender<Message>,
        fsm_tx: UnboundedSender<Instruction>,
    ) -> Result<Raft<Follower>> {
        config.validate()?;
        let chain = Chain::new(&config.data_directory)?;
        let mut raft = Raft {
            id: config.id,
            config,
            state: State::default(),
            role: Follower {
                leader_id: None,
                proxied_reqs: HashSet::new(),
                queued_reqs: Vec::new(),
            },
            chain,
            rpc_tx,
            fsm_tx,
        };

        raft.init();
        Ok(raft)
    }

    fn init(&mut self) {
        self.set_election_timeout();
    }

    fn can_vote(&self, last_term: Term, head: BlockId) -> bool {
        !(self.state.voted_for.is_some()
            || self.state.current_term > last_term
            || self.chain.get_commit() > head)
    }

    fn get_randomized_timeout(&self) -> Duration {
        let _prev_timeout = self.state.election_timeout;
        let timeout = rand::thread_rng()
            .gen_range(self.state.min_election_timeout..self.state.max_election_timeout);
        Duration::from_millis(timeout as u64)
    }

    fn set_election_timeout(&mut self) {
        self.state.election_timeout = Some(self.get_randomized_timeout());
        self.state.election_time = Some(Instant::now());
    }

    fn apply_self(self) -> Result<RaftHandle> {
        Ok(RaftHandle::Follower(self))
    }

    // Commands

    fn apply_tick(self) -> Result<RaftHandle> {
        if self.needs_election() {
            tracing::info!("timeout");
            return self.apply(Command::Timeout);
        }

        self.apply_self()
    }

    fn apply_append_entries(
        mut self,
        blocks: Vec<Block>,
        leader_id: NodeId,
        term: Term,
    ) -> Result<RaftHandle> {
        // If we haven't voted and the rpc term is greater, set term to that term.
        if self.state.voted_for.is_none() && term >= self.state.current_term {
            self.term(term);

            // Vote for leader and reset election timeout
            self.state.election_time = Some(Instant::now());
            self.role.leader_id = Some(leader_id);
            self.state.voted_for = Some(leader_id);
        }

        // If we voted for someone...
        if let Some(voted_for) = self.state.voted_for {
            // And the entries rpc is from another "leader" with a lower term
            assert!(
                !(voted_for != leader_id && term < self.state.current_term),
                "{:?}",
                ..
            );
        }

        // If there are entries...
        if !blocks.is_empty() {
            for block in blocks {
                self.chain.extend(block)?; // append the entry
            }

            // confirm append
            self.rpc_tx.send(Message::new(
                Address::Peer(self.id),
                Address::Peer(leader_id),
                Command::AppendResponse {
                    node_id: self.id,
                    term: self.state.current_term,
                    head: self.chain.get_head(),
                    success: true,
                },
            ))?;
        }

        self.apply_self()
    }

    fn apply_heartbeat(
        mut self,
        leader_id: NodeId,
        term: Term,
        commit: BlockId,
    ) -> Result<RaftHandle> {
        self.set_election_timeout();
        self.term(term);
        self.role.leader_id = Some(leader_id);
        self.state.voted_for = Some(leader_id);

        // send any queued requests
        for req in std::mem::replace(&mut self.role.queued_reqs, vec![]).into_iter() {
            let id = req.id;
            self.send(
                Address::Peer(leader_id),
                Command::ClientRequest(req.clone()),
            )?;
            self.role.proxied_reqs.insert(id);
        }

        // apply entries to state machine if leader has advanced commit index
        let has_committed = self.chain.has(&commit)?;
        if has_committed && &commit > &self.chain.get_commit()  {
            let prev = self.chain.get_commit();
            self.chain.commit(&commit)?;
            self.chain.range(prev..commit).for_each(|block| {
                self.fsm_tx.send(Instruction::Apply { block }).unwrap();
            });
        }

        self.send(
            Address::Peer(leader_id),
            Command::HeartbeatResponse {
                commit: self.chain.get_commit(),
                has_committed,
            },
        )?;
        self.apply_self()
    }

    fn apply_vote_request(
        mut self,
        candidate_id: NodeId,
        last_term: Term,
        head: BlockId,
    ) -> Result<RaftHandle> {
        if self.can_vote(last_term, head) {
            self.send(
                Address::Peer(candidate_id),
                VoteResponse {
                    term: self.state.current_term,
                    from: self.id,
                    granted: true,
                },
            )?;
            self.state.voted_for = Some(candidate_id);
        } else {
            self.send(
                Address::Peer(candidate_id),
                VoteResponse {
                    term: self.state.current_term,
                    from: self.id,
                    granted: false,
                },
            )?;
        }
        self.apply_self()
    }

    fn apply_timeout(mut self) -> Result<RaftHandle> {
        if self.state.voted_for.is_none() {
            self.set_election_timeout(); // start a new election
            let raft: Raft<Candidate> = Raft::from(self);
            return raft.seek_election();
        }

        self.apply_self()
    }

    fn apply_client_request(
        mut self,
        mut req: ClientRequest,
    ) -> Result<RaftHandle> {
        // rewrite address to our own so we can close out the request ourself
        req.address = Address::Peer(self.id);
        if let Some(leader_id) = self.role.leader_id {
            let id = req.id;
            self.send(
                Address::Peer(leader_id),
                Command::ClientRequest(req),
            )?;
            self.role.proxied_reqs.insert(id);
        } else {
            self.role.queued_reqs.push(req);
        }
        self.apply_self()
    }

    fn apply_client_response(
        mut self,
        id: ClientRequestId,
        res: Result<Response, ResponseError>,
    ) -> Result<RaftHandle> {
        self.send(Address::Client, Command::ClientResponse(ClientResponse { id, res }))?;
        self.role.proxied_reqs.remove(&id);
        self.apply_self()
    }
}

impl From<Raft<Follower>> for Raft<Candidate> {
    fn from(val: Raft<Follower>) -> Raft<Candidate> {
        let mut node_ids: Vec<NodeId> = val.config.nodes.iter().map(|n| n.id).collect();
        node_ids.push(val.id);
        let election = Election::new(node_ids);

        Raft {
            id: val.id,
            state: val.state,
            role: Candidate { election, queued_reqs: Vec::new() },
            config: val.config,
            chain: val.chain,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;
    use super::Command;
    use super::RaftHandle;
    use crate::raft::chain::BlockId;
    use crate::raft::test::new_follower;
    use crate::raft::Apply;

    #[test]
    fn follower_to_leader() {
        let ((_rpc_rx, _fsm_rx), follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let (_, follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }

    #[tokio::test]
    async fn apply_heartbeat() -> anyhow::Result<()> {
        let ((mut rpc_rx, _), follower) = new_follower();
        let follower = follower
            .apply_heartbeat(11, 12, BlockId::new(1))?
            .get_follower()
            .unwrap();
        // we voted for the leader
        assert!(follower.state.voted_for.is_some());
        assert_eq!(follower.state.voted_for.unwrap(), 11);
        assert_eq!(follower.state.current_term, 12);
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

    #[tokio::test]
    async fn apply_vote_request() -> anyhow::Result<()> {
        let ((mut rpc_rx, _), follower) = new_follower();
        let mut follower = follower
            .apply_vote_request(11, 12, BlockId::new(1))?
            .get_follower()
            .unwrap();
        // we voted for the leader
        assert!(follower.state.voted_for.is_some());
        assert_eq!(follower.state.voted_for.unwrap(), 11);
        let msg = rpc_rx.recv().await.unwrap();
        // we granted the request
        assert_eq!(
            msg.command,
            Command::VoteResponse {
                term: 0,
                from: 1,
                granted: true
            }
        );
        follower.state.voted_for = Some(2);
        let _follower = follower
            .apply_vote_request(11, 12, BlockId::new(1))
            .unwrap();
        let msg = rpc_rx.recv().await.unwrap();
        // we already voted
        assert_eq!(
            msg.command,
            Command::VoteResponse {
                term: 0,
                from: 1,
                granted: false
            }
        );
        Ok(())
    }

    #[test]
    fn apply_timeout() -> anyhow::Result<()> {
        let ((_rpc_rx, _), follower) = new_follower();
        let _leader = follower
            .apply_timeout()?
            .get_leader()
            .unwrap();

        Ok(())
    }

    #[test]
    fn apply_tick() -> anyhow::Result<()> {
        let ((_rpc_rx, _), mut follower) = new_follower();
        follower.state.election_time = Some(Instant::now());
        follower.state.election_timeout = Some(follower.config.election_timeout);
        let follower = follower
            .apply_tick()?
            .get_follower()
            .unwrap();
        std::thread::sleep(follower.config.election_timeout);
        let _leader = follower
            .apply_tick()?
            .get_leader()
            .unwrap();
        Ok(())
    }

    #[test]
    fn apply_append_entries() -> anyhow::Result<()> {
        let ((_rpc_rx, _), mut follower) = new_follower();
        follower.state.election_time = Some(Instant::now());
        follower.state.election_timeout = Some(follower.config.election_timeout);
        let follower = follower
            .apply_tick()?
            .get_follower()
            .unwrap();
        std::thread::sleep(follower.config.election_timeout);
        let _leader = follower
            .apply_tick()?
            .get_leader()
            .unwrap();
        Ok(())
    }
}
