use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use slog::Logger;

use crate::follower::Follower;
use crate::progress::NodeProgress;
use crate::progress::ReplicationProgress;
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::raft::Term;
use crate::raft::{Apply, NodeId, RaftHandle, RaftRole};
use crate::{
    error::{RaftError, Result},
    fsm,
    raft::LogIndex,
};

///
#[derive(Debug)]
pub struct Leader {
    pub logger: Logger,
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

    fn log(&self) -> &Logger {
        &self.logger
    }
}

impl Raft<Leader> {
    pub(crate) fn heartbeat(&self) -> Result<()> {
        self.send_all(Command::Heartbeat {
            term: self.state.current_term,
            leader_id: self.id,
        })?;
        Ok(())
    }

    fn _append_entry(&mut self, _node_id: NodeId, handle: NodeProgress) {
        match handle {
            NodeProgress::Probe(_) => {}
            NodeProgress::Replicate(_) => {}
            NodeProgress::Snapshot(_) => {}
        };
    }

    fn needs_heartbeat(&self) -> bool {
        self.role.heartbeat_time.elapsed() > self.role.heartbeat_timeout
    }

    fn reset_heartbeat_timer(&mut self) {
        self.role.heartbeat_time = Instant::now();
    }

    fn commit(&mut self) -> Result<LogIndex> {
        let quorum_idx = self.role.progress.committed_index();
        if quorum_idx > self.state.commit_index
            && self.log.check_term(quorum_idx, self.state.current_term)
        {
            self.log.commit(quorum_idx);
            let prev = self.state.commit_index;
            self.state.commit_index = quorum_idx;
            self.log
                .get_range(prev, self.state.commit_index)?
                .into_iter()
                .for_each(|entry| self.fsm_tx.send(fsm::Instruction::Drive { entry }).unwrap());
        }

        Ok(quorum_idx)
    }

    fn write_state(&self) {
        #[derive(Serialize)]
        struct RaftDebugState {
            leader_id: NodeId,
            term: Term,
            index: LogIndex,
        }

        let tmp = std::env::temp_dir();
        let state_file = tmp.join("josefine.json");
        let mut state_file = std::fs::File::create(state_file).expect("couldn't create state file");

        let debug_state = RaftDebugState { leader_id: self.id, index: self.state.commit_index, term: self.state.current_term };
        state_file
            .write(&serde_json::to_vec(&debug_state).expect("could not serialize state"))
            .expect("could not write state");
    }
}

impl Apply for Raft<Leader> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                self.write_state();
                
                if self.needs_heartbeat() {
                    if let Err(_err) = self.heartbeat() {
                        panic!("Could not heartbeat")
                    }
                    self.reset_heartbeat_timer();
                }

                for node in &self.config.nodes {
                    if let Some(mut progress) = self.role.progress.get_mut(node.id) {
                        match &mut progress {
                            NodeProgress::Replicate(progress) => {
                                let entries = self.log.get_range(
                                    progress.next,
                                    progress.next + crate::progress::MAX_INFLIGHT,
                                )?;
                                let len = entries.len();
                                // let _ = Message::Append {
                                //     term: self.state.current_term,
                                //     leader_id: self.id,
                                //     prev_log_index: progress.index,
                                //     prev_log_term: 0, // TODO(jcm) need to track in progress?
                                //     entries: entries.to_vec(),
                                //     leader_commit: 0,
                                // };

                                progress.next = len as u64;
                            }
                            _ => {}
                        }
                    }
                }

                Ok(RaftHandle::Leader(self))
            }
            Command::AppendResponse { node_id, index, .. } => {
                if let Some(mut progress) = self.role.progress.get_mut(node_id) {
                    match &mut progress {
                        NodeProgress::Replicate(progress) => {
                            progress.increment(index);
                        }
                        _ => panic!(),
                    }
                }

                self.state.commit_index = self.role.progress.committed_index();
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
                logger: val.logger.new(o!("role" => "follower")),
            },
            logger: val.logger,
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}
