use std::collections::HashMap;
use std::io::BufReader;
use std::io::Read;
use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::str;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use slog::*;

use crate::config::RaftConfig;
use crate::raft::Apply;
use crate::raft::Command;
use crate::raft::Io;
use crate::raft::MemoryIo;
use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::RaftHandle;
use crate::rpc::Message;
use crate::rpc::TpcRpc;
use crate::raft::NodeMap;
use std::net::SocketAddr;

/// A server implementation that wraps the Raft state machine and handles connection with other nodes via a TPC
/// RPC implementation.
///
/// The server handles wiring up the state machine and driving it forward at a fixed interval.
pub struct RaftServer {
    raft: RaftHandle<MemoryIo, TpcRpc>,
    config: RaftConfig,
    log: Logger,
    tx: Sender<Command>,
    rx: Receiver<Command>,
}

impl RaftServer {
    /// Creates a new server that wraps the Raft state machine and handles driving the state machine
    /// forward on the basis of some external input.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for this raft instance.
    /// * `logger` - The root logger to use for the application.
    ///
    ///
    /// # Example
    ///
    /// ```
    /// let server = RaftServer::new(Config::default(), logger);
    /// ```
    pub fn new(config: RaftConfig, logger: Logger) -> RaftServer {
        let log = logger.new(o!());
        let (tx, rx) = channel::<Command>();

        let nodes: HashMap<NodeId, Node> = config.nodes.iter()
            .map(|x| {
                Node {
                    id: 0,
                    addr: x.parse().expect("Could not parse address"),

                }
            })
            .map(|x| (x.id, x))
            .collect();

        let nodes = Arc::new(RwLock::new(nodes));

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

    /// Start the server and the state machine. Raft is driven every 100 milliseconds.
    pub fn start(self) {
        self.listen();

        let mut timeout = Duration::from_millis(100);
        let mut t = Instant::now();
        let mut raft = self.raft.apply(Command::Start).unwrap();

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
                    Ok(stream) => {
                        let reader = BufReader::new(&stream);
                        let msg: Message = serde_json::from_reader(reader).unwrap();
                        info!(log, ""; "message" => format!("{:?}", msg));
                        let cmd = match msg {
                            Message::AddNodeRequest(node) => {
                                Command::AddNode(node)
                            }
                            Message::AppendRequest(req) => {
                                Command::Append {
                                    term: req.term,
                                    leader_id: 0,
                                    entries: vec![],
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