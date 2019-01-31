use std::collections::HashMap;
use std::io::BufReader;
use std::io::Read;
use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::str;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use slog::*;

use crate::log;
use crate::config::RaftConfig;
use crate::raft::Apply;
use crate::raft::Command;
use crate::io::Io;
use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::RaftHandle;
use crate::rpc::Message;
use crate::rpc::TpcRpc;
use crate::raft::NodeMap;
use std::net::SocketAddr;
use crate::io::MemoryIo;

/// A server implementation that wraps the Raft state machine and handles connection with other nodes via a TPC
/// RPC implementation.
///
/// The server handles wiring up the state machine and driving it forward at a fixed interval.
pub struct RaftServer {
    pub(crate) raft: RaftHandle<MemoryIo, TpcRpc>,
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
    /// #[macro_use]
    /// extern crate slog;
    /// extern crate slog_async;
    /// extern crate slog_term;
    ///
    /// use josefine_raft::server::RaftServer;
    /// use josefine_raft::config::RaftConfig;
    /// use slog::Drain;
    /// use slog::Logger;
    ///
    /// fn main() {
    ///     let decorator = slog_term::TermDecorator::new().build();
    ///     let drain = slog_term::FullFormat::new(decorator).build().fuse();
    ///     let drain = slog_async::Async::new(drain).build().fuse();
    ///
    ///     let logger = Logger::root(drain, o!());
    ///
    ///     let server = RaftServer::new(RaftConfig::default(), logger);
    /// }
    /// ```
    pub fn new(config: RaftConfig, logger: Logger) -> RaftServer {
        let log = logger.new(o!());
        let (tx, rx) = channel::<Command>();

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        let io = MemoryIo::new();
        let rpc = TpcRpc::new(config.clone(), tx.clone(), nodes.clone(), log.new(o!()));
        let raft = RaftHandle::new(config.clone(), tx.clone(), io, rpc, logger, nodes.clone());

        RaftServer {
            raft,
            config,
            log,
            tx,
            rx,
        }
    }

    /// Start the server and the state machine. Raft is driven every 100 milliseconds.
    pub fn start(self, run_for: Option<Duration>) -> RaftHandle<MemoryIo, TpcRpc> {
        self.listen();

        let mut timeout = Duration::from_millis(100);
        let mut t = Instant::now();
        let mut raft = self.raft.apply(Command::Start).unwrap();

        let now = Instant::now();

        loop {
            if let Some(run_for) = run_for {
                if now.elapsed() > run_for {
                    break;
                }
            }

            match self.rx.recv_timeout(timeout) {
                Ok(cmd) => {
                    raft = raft.apply(cmd).unwrap();
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return raft,
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

        raft
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
                        let msg: Message = match serde_json::from_reader(reader) {
                            Ok(msg) => msg,
                            Err(e) => {
                                info!(log, "Client disconnected."; "address" => format!("{:?}", stream.peer_addr().unwrap()));
                                continue;
                            }
                        };

                        info!(log, ""; "message" => format!("{:?}", msg));
                        let cmd = match msg {
                            Message::AddNodeRequest(socket_addr) => {
                                Command::AddNode(socket_addr)
                            }
                            Message::AppendRequest(req) => {
                                Command::AppendEntries {
                                    term: req.term,
                                    leader_id: 0,
                                    entries: vec![],
                                }
                            }
                            Message::AppendResponse(res) => {
                                Command::AppendResponse {
                                    node_id: res.header.node_id,
                                    term: res.term,
                                    index: res.last_log,
                                }
                            }
                            Message::VoteRequest(req) => {
                                Command::VoteRequest {
                                    term: req.term,
                                    candidate_id: req.candidate_id,
                                    last_term: req.last_term,
                                    last_index: req.last_index,
                                }
                            }
                            _ => Command::Noop,
                        };
                        tx.send(cmd).expect("Channel should be open");
                    }
                    Err(e) => { panic!(e) }
                }
            }
        });
    }
}


#[cfg(test)]
mod tests {
    use crate::server::RaftServer;
    use slog::Logger;
    use crate::config::RaftConfig;
    use crate::log;
    use crate::raft::RaftHandle;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn it_runs() {
        let t = thread::spawn(|| {
            let election_timeout_max = 1000; // TODO: Expose constants better

            let config = RaftConfig::default();
            let server = RaftServer::new(config, log::get_root_logger());
            let server = server.start(Some(Duration::from_millis(election_timeout_max)));
            server
        });

        let server = t.join().unwrap();
        match server {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => {}
        }
    }
}
