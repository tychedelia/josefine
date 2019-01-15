use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::sync::mpsc::channel;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

use slog::*;

use crate::config::RaftConfig;
use crate::raft::Apply;
use crate::raft::Command;
use crate::raft::Io;
use crate::raft::MemoryIo;
use crate::raft::Raft;
use crate::raft::RaftHandle;
use crate::rpc::TpcRpc;

pub struct RaftServer {
    pub raft: RaftHandle<MemoryIo, TpcRpc>,
    config: RaftConfig,
}

impl RaftServer {
    pub fn new(config: RaftConfig) -> RaftServer {
        let raft = RaftHandle::Follower(Raft::new(config.clone(), MemoryIo::new(), TpcRpc::new(config.clone())).unwrap());

        RaftServer {
            raft,
            config,
        }
    }

    pub fn start(self) {
//        info!("Starting {}:{}", self.config.ip, self.config.port);

        let address = format!("{}:{}", self.config.ip, self.config.port);
        let listener = TcpListener::bind(address).unwrap();

        let timeout = Duration::from_millis(100);


        let (tx, rx) = channel::<Command>();

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(_stream) => {
//                        info!("Stream!");
                        tx.send(Command::Ping(0)).unwrap();
                    }
                    Err(e) => { panic!(e) }
                }
            }
        });


        let mut raft = self.raft;

        info!(raft.log(), "Starting"; "address" => format!("{}:{}", self.config.ip, self.config.port));

        loop {
            match rx.recv_timeout(timeout) {
                Ok(cmd) => {
                    raft = raft.apply(cmd).unwrap();
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::thread;

    use crate::config::RaftConfig;
    use crate::server::RaftServer;

    #[test]
    fn test() {
        thread::spawn(|| {
            let server = RaftServer::new(RaftConfig::default());
            server.start();
        });

        thread::spawn(|| {
            let server = RaftServer::new(RaftConfig {
                port: 6668,
                ..Default::default()
            });
            server.start();
        });

        loop {}
    }
}