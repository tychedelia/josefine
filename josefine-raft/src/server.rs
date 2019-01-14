use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use log::{info, trace, warn};

use crate::config::Config;
use crate::raft::Apply;
use crate::raft::Command;
use crate::raft::Io;
use crate::raft::MemoryIo;
use crate::raft::NodeId;
use crate::raft::Raft;
use crate::raft::RaftHandle;
use crate::rpc::ChannelMap;
use crate::rpc::Message;
use crate::rpc::PORT;
use crate::rpc::Rpc;
use crate::rpc::TpcRpc;

struct RaftServer {
    raft: RaftHandle<MemoryIo, TpcRpc>,
    config: Config,
}

impl RaftServer {
    fn new() -> RaftServer {
        let config = Config::default();
        let raft = RaftHandle::Follower(Raft::new(config, MemoryIo::new(), TpcRpc::new(config)).unwrap());

        RaftServer {
            raft,
            config,
        }
    }

    pub fn start(self) {
        info!("Starting {}", self.config.id);
        let ip = Ipv4Addr::from(self.config.id);
        let address = format!("{}:{}", ip, PORT);
        let listener = TcpListener::bind(address).unwrap();

        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(100);


        let (tx, rx) = channel::<Command>();

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        info!("Stream!");
                        tx.send(Command::Noop).unwrap();
                    }
                    Err(e) => { panic!(e) }
                }
            }
        });


        let mut raft = self.raft;

        loop {
            match rx.recv_timeout(timeout) {
                Ok(cmd) => {
                    raft = raft.apply(cmd).unwrap();
                },
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::thread;

    use crate::server::RaftServer;

    #[test]
    fn test() {
        thread::spawn(|| {
            let server = RaftServer::new();
            server.start();
        });

        thread::spawn(|| {
            let server = RaftServer::new();
            server.start();
        });

        loop {

        }
    }
}