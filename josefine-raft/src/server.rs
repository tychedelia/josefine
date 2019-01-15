use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::sync::mpsc::channel;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;
use std::io::Read;

use slog::*;

use crate::config::RaftConfig;
use crate::raft::Apply;
use crate::raft::Command;
use crate::raft::Io;
use crate::raft::MemoryIo;
use crate::raft::Raft;
use crate::raft::RaftHandle;
use crate::rpc::TpcRpc;
use crate::raft::Node;
use std::collections::HashMap;
use crate::raft::NodeId;
use std::sync::Arc;
use std::sync::Mutex;
use std::cell::RefCell;
use std::rc::Rc;

pub struct RaftServer {
    pub raft: RaftHandle<MemoryIo, TpcRpc>,
    config: RaftConfig,
    log: Logger,
}

impl RaftServer {
    pub fn new(config: RaftConfig, logger: Logger) -> RaftServer {
        let log = logger.new(o!());

        let nodes: HashMap<NodeId, Node> = config.nodes.iter()
            .map(|x| {
                let parts: Vec<&str> = x.split(":").collect();

                Node {
                id: 0,
                ip: parts[0].parse().unwrap(),
                port: parts[1].parse().unwrap(),
            }})
            .map(|x| (x.id, x))
            .collect();

        let nodes = Rc::new(RefCell::new(nodes));

        let io = MemoryIo::new();
        let rpc = TpcRpc::new(config.clone(), nodes.clone());
        let raft = RaftHandle::new(config.clone(), io, rpc, logger, nodes.clone());

        RaftServer {
            raft,
            config,
            log,
        }
    }

    pub fn start(self) {
        let address = format!("{}:{}", self.config.ip, self.config.port);
        info!(self.log, "Listening"; "address" => &address);

        let listener = TcpListener::bind(&address).unwrap();
        let timeout = Duration::from_millis(100);
        let (tx, rx) = channel::<Command>();

        let log = self.log.new(o!());
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        info!(log, ""; "message" => stream.read(&mut [0; 128]).unwrap());
                        tx.send(Command::Ping(69)).unwrap();
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
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}