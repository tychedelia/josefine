use std::io::Error;

use slog::Logger;

use crate::follower::Follower;
use crate::raft::{Apply, RaftHandle};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;
use crate::progress::ReplicationProgress;
use std::time::Instant;
use std::time::Duration;
use rand::Rng;
use crate::io::Io;
use crate::progress::ProgressHandle;
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
                let entries = self.io.entries_from(progress.index);

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

}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Leader, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, failure::Error> {
        trace!(self.role.log, "Applying command"; "command" => format!("{:?}", command));

        match command {
            Command::Tick => {
                if self.needs_heartbeat() {
                    if let Err(err) = self.heartbeat() {
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
            Command::AppendResponse { node_id, term, index } => {
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
            Command::AppendEntries { term, leader_id, entries } => {
                if term > self.state.current_term {
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