use std::io::Error;

use slog::Logger;

use crate::follower::Follower;
use crate::raft::{Apply, Io, RaftHandle};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;
use crate::progress::ReplicationProgress;
use std::time::Instant;
use std::time::Duration;
use rand::Rng;

//
pub struct Leader {
    pub log: Logger,
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl<I: Io, R: Rpc> Raft<Leader, I, R> {
    fn heartbeat(&self) {
        self.rpc.heartbeat(self.state.current_term, self.state.commit_index, vec!());
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
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, Error> {
        match command {
            Command::Tick => {
                if self.needs_heartbeat() {
                    if let Err(err) = self.rpc.heartbeat(self.state.current_term, self.state.commit_index, vec![]) {
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
            Command::AppendEntries { term, leader_id, .. } => {
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
//        info!("{} transitioning from leader to follower", val.id);

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