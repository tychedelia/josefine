

use std::time::Duration;
use std::time::Instant;


use slog::Logger;

use crate::follower::Follower;
use crate::progress::ProgressHandle;
use crate::progress::ReplicationProgress;
use crate::raft::{Apply, RaftHandle, RaftRole, NodeId};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::RpcMessage;

///
#[derive(Debug)]
pub struct Leader {
    pub log: Logger,
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl Raft<Leader> {
    pub(crate) fn heartbeat(&self) -> Result<(), failure::Error> {
        for (_, node) in &self.nodes {
            node.try_send(RpcMessage::Heartbeat(self.state.current_term, self.id))?;
        };

        Ok(())
    }

    fn append_entry(&mut self, node_id: NodeId, handle: ProgressHandle) {
        match handle {
            ProgressHandle::Probe(probe) => {},
            ProgressHandle::Replicate(_) => {},
            ProgressHandle::Snapshot(_) => {},
        };
    }
//
//    fn append_entries(&mut self) {
//        for (id, _) in nodes {
//            self.append_entry(id, self.role.progress[id]);
//        }
//    }

    fn needs_heartbeat(&self) -> bool {
        self.role.heartbeat_time.elapsed() > self.role.heartbeat_timeout
    }

    fn reset_heartbeat_timer(&mut self) {
        self.role.heartbeat_time = Instant::now();
    }
}

impl Role for Leader {
    fn term(&mut self, _term: u64) {
    }

    fn role(&self) -> RaftRole {
        RaftRole::Leader
    }

    fn log(&self) -> &Logger {
        &self.log
    }
}

impl Apply for Raft<Leader> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle, failure::Error> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                if self.needs_heartbeat() {
                    if let Err(_err) = self.heartbeat() {
                        panic!("Could not heartbeat")
                    }
                    self.reset_heartbeat_timer();
                }
                Ok(RaftHandle::Leader(self))
            }
            Command::AddNode(_node) => {
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendResponse { node_id, term: _, index } => {
                if let Some(mut progress) = self.role.progress.get_mut(node_id) {
                    match &mut progress {
                        ProgressHandle::Replicate(progress) => {
                            progress.increment(index);
                        }
                        _ => panic!()
                    }
                }

                self.state.commit_index = self.role.progress.committed_index();
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendEntries { term, leader_id: _, entries: _ } => {
                if term > self.state.current_term {
                    // TODO:
                    self.term(term);
                    return Ok(RaftHandle::Follower(Raft::from(self)));
                }

                Ok(RaftHandle::Leader(self))
            }
            _ => Ok(RaftHandle::Leader(self))
        }
    }
}

impl From<Raft<Leader>> for Raft<Follower> {
    fn from(val: Raft<Leader>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            role: Follower { leader_id: None, log: val.log.new(o!("role" => "follower"))  },
            log: val.log,
            config: val.config
        }
    }
}