use std::io::Error;
use std::sync::mpsc::Sender;
use std::time::Duration;
use std::time::Instant;

use rand::Rng;
use slog::Logger;

use crate::follower::Follower;
use crate::progress::ProgressHandle;
use crate::progress::ReplicationProgress;
use crate::raft::{Apply, RaftHandle, RaftRole};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;

///
pub struct Leader {
    pub log: Logger,
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl Raft<Leader> {
    fn heartbeat(&self) -> Result<(), failure::Error> {
        Ok(())
    }

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
}

impl Apply for Raft<Leader> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle, failure::Error> {
        trace!(self.role.log, "Applying command"; "command" => format!("{:?}", cmd));
        match cmd{
            Command::Tick => {
                if self.needs_heartbeat() {
                    if let Err(_err) = self.heartbeat() {
                        panic!("Could not heartbeat")
                    }
                    self.reset_heartbeat_timer();
                }
                Ok(RaftHandle::Leader(self))
            }
            Command::AddNode(node) => {
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