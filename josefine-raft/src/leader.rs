use std::io::Error;
use std::sync::mpsc::Sender;
use std::time::Duration;
use std::time::Instant;

use rand::Rng;
use slog::Logger;

use crate::follower::Follower;
use crate::io::Io;
use crate::progress::ProgressHandle;
use crate::progress::ReplicationProgress;
use crate::raft::{Apply, ApplyResult, ApplyStep, RaftHandle, RaftRole};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;
use crate::rpc::RpcError;

///
pub struct Leader {
    pub log: Logger,
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl<I: Io, R: Rpc> Raft<Leader, I, R> {
    fn heartbeat(&self) -> Result<(), RpcError> {
        for node_id in self.nodes.read().unwrap().keys() {
            if let ProgressHandle::Replicate(progress) = self.role.progress.get(*node_id).unwrap() {
                let _entries = self.io.entries_from(progress.index);

                self.rpc.heartbeat(*node_id, self.state.current_term, self.state.commit_index, &vec!())?;
            }
        }

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

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Leader, I, R> {
    fn apply(mut self, step: ApplyStep) -> Result<RaftHandle<I, R>, failure::Error> {
        let ApplyStep(command, _cb) = step;
        trace!(self.role.log, "Applying command"; "command" => format!("{:?}", command));
        match command {
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
                self.add_node_to_cluster(node);
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

impl<I: Io, R: Rpc> From<Raft<Leader, I, R>> for Raft<Follower, I, R> {
    fn from(val: Raft<Leader, I, R>) -> Raft<Follower, I, R> {
        Raft {
            id: val.id,
            state: val.state,
            tx: val.tx,
            nodes: val.nodes,
            io: val.io,
            rpc: val.rpc,
            role: Follower { leader_id: None, log: val.log.new(o!("role" => "follower"))  },
            log: val.log,
            config: val.config
        }
    }
}