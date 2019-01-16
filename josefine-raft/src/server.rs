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
use std::str;
use crate::rpc::Message;
use std::io::BufReader;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::time::Instant;

pub struct RaftServer {
    pub raft: RaftHandle<MemoryIo, TpcRpc>,
    config: RaftConfig,
    log: Logger,
    tx: Sender<Command>,
    rx: Receiver<Command>,
}

impl RaftServer {
    pub fn new(config: RaftConfig, logger: Logger) -> RaftServer {
        let log = logger.new(o!());
        let (tx, rx) = channel::<Command>();

        let nodes: HashMap<NodeId, Node> = config.nodes.iter()
            .map(|x| {
                let parts: Vec<&str> = x.split(':').collect();

                Node {
                id: 0,
                ip: parts[0].parse().unwrap(),
                port: parts[1].parse().unwrap(),
            }})
            .map(|x| (x.id, x))
            .collect();

        let nodes = Rc::new(RefCell::new(nodes));

        let io = MemoryIo::new();
        let rpc = TpcRpc::new(config.clone(), tx.clone(), nodes.clone(), log.new(o!()));
        let raft = RaftHandle::new(config.clone(), io, rpc, logger, nodes.clone());

        RaftServer {
            raft,
            config,
            log,
            tx,
            rx,
        }
    }

    pub fn start(self) {
        self.listen();

        let mut timeout = Duration::from_millis(100);
        let mut t = Instant::now();
        let mut raft = self.raft;

        loop {
            match self.rx.recv_timeout(timeout) {
                Ok(cmd) => {
                    raft = raft.apply(cmd).unwrap();
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }

            let d = t.elapsed();
            t = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(100);
                raft = raft.apply(Command::Tick).unwrap();
            } else {
                timeout -= d;
            }
        }
    }

    fn listen(&self) {
        let address = format!("{}:{}", self.config.ip, self.config.port);
        info!(self.log, "Listening"; "address" => &address);

        let listener = TcpListener::bind(&address).unwrap();
        let tx = self.tx.clone();
        let log = self.log.new(o!());

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let reader = BufReader::new(&stream);
                        let msg: Message = serde_json::from_reader(reader).unwrap();
                        trace!(log, ""; "message" => format!("{:?}", msg));
                        let cmd = match msg {
                            Message::AddNodeRequest(node) => {
                                Command::AddNode(node)
                            }
                            Message::AppendRequest(req) => {
                                Command::Append {
                                    term: req.term,
                                    from: 0,
                                    entries: vec![]
                                }
                            }
                            _ => Command::Noop,
                        };
                        tx.send(cmd);
                    }
                    Err(e) => { panic!(e) }
                }
            }
        });
    }
}