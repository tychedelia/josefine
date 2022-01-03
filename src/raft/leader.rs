use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;

use crate::raft::follower::Follower;
use crate::raft::progress::ReplicationProgress;
use crate::raft::progress::{NodeProgress, MAX_INFLIGHT};

use crate::raft::{Command, Raft};

use crate::raft::chain::{BlockId, UnappendedBlock};
use crate::raft::fsm::Instruction;
use crate::raft::rpc::Address;
use crate::raft::rpc::Message;
use crate::raft::Role;
use crate::raft::Term;
use crate::raft::{Apply, NodeId, RaftHandle, RaftRole};
use std::collections::HashSet;

///
#[derive(Debug)]
pub struct Leader {
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl Role for Leader {
    fn term(&mut self, _term: u64) {
        unimplemented!()
    }

    fn role(&self) -> RaftRole {
        RaftRole::Leader
    }
}

impl Raft<Leader> {
    pub(crate) fn heartbeat(&self) -> Result<()> {
        self.send_all(Command::Heartbeat {
            term: self.state.current_term,
            commit: self.chain.get_commit(),
            leader_id: self.id,
        })?;
        Ok(())
    }

    pub(crate) fn on_transition(self) -> Result<Raft<Leader>> {
        // let term = self.state.current_term;
        // let next_index = self.log.next_index();
        // let entry = Entry {
        //     entry_type: EntryType::Control,
        //     term,
        //     index: next_index,
        // };
        // self.state.last_applied = self.log.append(entry)?;
        //
        // let node_id = self.id;
        // let index = self.state.last_applied;
        // self.send(
        //     Address::Local,
        //     Command::AppendResponse {
        //         node_id,
        //         term,
        //         index,
        //         success: true,
        //     },
        // )?;

        Ok(self)
    }

    fn needs_heartbeat(&self) -> bool {
        self.role.heartbeat_time.elapsed() > self.role.heartbeat_timeout
    }

    fn reset_heartbeat_timer(&mut self) {
        self.role.heartbeat_time = Instant::now();
    }

    #[tracing::instrument]
    fn commit(&mut self) -> Result<BlockId> {
        let quorum_idx = self.role.progress.committed_index();
        tracing::trace!(?quorum_idx, "commit");
        if quorum_idx > self.chain.get_commit()
        // && self.chain.check_term(quorum_idx, self.state.current_term)
        {
            let prev = self.chain.get_commit();
            let new = self.chain.commit(&quorum_idx)?;
            self.chain.range(prev..=new).for_each(|block| {
                self.fsm_tx.send(Instruction::Apply { block }).unwrap();
            });
        }

        Ok(quorum_idx)
    }

    fn write_state(&self) {
        #[derive(Serialize)]
        struct RaftDebugState {
            leader_id: NodeId,
            commit: BlockId,
            term: Term,
        }

        let tmp = std::env::temp_dir();
        let state_file = tmp.join("josefine.json");
        let mut state_file = std::fs::File::create(state_file).expect("couldn't create state file");

        let debug_state = RaftDebugState {
            leader_id: self.id,
            commit: self.chain.get_commit(),
            term: self.state.current_term,
        };
        state_file
            .write_all(&serde_json::to_vec(&debug_state).expect("could not serialize state"))
            .expect("could not write state");
    }

    fn replicate(&mut self) -> Result<()> {
        for node in &self.config.nodes {
            if let Some(mut progress) = self.role.progress.get_mut(node.id) {
                if !progress.is_active() {
                    continue;
                }

                match &mut progress {
                    NodeProgress::Probe(progress) => {
                        let blocks =
                            if let Some(block) = self.chain.range(progress.head.clone()..).next() {
                                vec![block]
                            } else {
                                vec![]
                            };

                        self.rpc_tx.send(Message::new(
                            Address::Peer(self.id),
                            Address::Peer(node.id),
                            Command::AppendEntries {
                                term: self.state.current_term,
                                leader_id: self.id,
                                blocks,
                            },
                        ))?;
                    }
                    NodeProgress::Replicate(progress) => {
                        let blocks = self
                            .chain
                            .range(progress.head.clone()..)
                            .take(MAX_INFLIGHT as usize)
                            .collect();
                        self.rpc_tx.send(Message::new(
                            Address::Peer(self.id),
                            Address::Peer(node.id),
                            Command::AppendEntries {
                                term: self.state.current_term,
                                leader_id: self.id,
                                blocks,
                            },
                        ))?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

impl Apply for Raft<Leader> {
    #[tracing::instrument]
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                self.write_state();

                if self.needs_heartbeat() {
                    self.heartbeat()?;
                    self.reset_heartbeat_timer();
                }

                self.replicate()?;

                Ok(RaftHandle::Leader(self))
            }
            Command::HeartbeatResponse {
                commit,
                has_committed,
            } => {
                if !has_committed && commit > BlockId::new(0) {
                    self.replicate()?;
                }
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendResponse { node_id, head, .. } => {
                self.role.progress.advance(node_id, head);
                self.commit()?;
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendEntries { term, .. } => {
                if term > self.state.current_term {
                    // TODO(jcm): move term logic into dedicated handler
                    self.term(term);
                    return Ok(RaftHandle::Follower(Raft::from(self)));
                }

                Ok(RaftHandle::Leader(self))
            }
            Command::ClientRequest {
                id,
                proposal,
                client_address,
            } => {
                let term = self.state.current_term;
                let block = UnappendedBlock::new(proposal.get());
                let block_id = self.chain.append(block)?;

                let node_id = self.id;

                self.fsm_tx.send(Instruction::Notify {
                    id,
                    block_id,
                    client_address,
                })?;

                let head = self.chain.get_head();
                self.apply(Command::AppendResponse {
                    node_id,
                    term,
                    success: true,
                    head,
                })
            }
            _ => Ok(RaftHandle::Leader(self)),
        }
    }
}

impl From<Raft<Leader>> for Raft<Follower> {
    fn from(val: Raft<Leader>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            role: Follower {
                leader_id: None,
                proxied_reqs: HashSet::new(),
            },
            config: val.config,
            chain: val.chain,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::rpc::Address;
    use crate::raft::test::new_follower;
    use crate::{
        raft::{fsm::Instruction, rpc::Proposal},
        raft::{Apply, Command, RaftHandle},
    };

    #[test]
    #[tracing_test::traced_test]
    fn apply_entry_single_node() {
        let ((_rpc_rx, mut fsm_rx), node) = new_follower();
        let node = node.apply(Command::Timeout).unwrap();
        assert!(node.is_leader());

        let magic_number = 123;

        let node = node
            .apply(Command::ClientRequest {
                id: vec![1],
                client_address: Address::Client,
                proposal: Proposal::new(vec![magic_number]),
            })
            .unwrap();
        let node = node.apply(Command::Tick).unwrap();
        if let RaftHandle::Leader(leader) = node {
            let block = leader.chain.range(..).take(2).last().unwrap();
            assert_eq!(block.data, vec![magic_number]);
            let _ = fsm_rx.blocking_recv().unwrap();
            // todo 2 vs 3
            let instruction = fsm_rx.blocking_recv().unwrap();
            let instruction = fsm_rx.blocking_recv().unwrap();
            if let Instruction::Apply { block } = instruction {
                assert_eq!(block.data, vec![magic_number]);
            } else {
                panic!()
            }
        } else {
            panic!()
        }
    }
}
