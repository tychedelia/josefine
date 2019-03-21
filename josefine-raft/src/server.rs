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
use crate::raft::{Apply, ApplyStep};
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
use hyper::{Server, rt, Response, Request, Body};
use tokio::prelude::future::Future;
use hyper::service::service_fn_ok;

/// A server implementation that wraps the Raft state machine and handles connection with other nodes via a TPC
/// RPC implementation.
///
/// The server handles wiring up the state machine and driving it forward at a fixed interval.
pub struct RaftServer {
    pub(crate) raft: RaftHandle<MemoryIo, TpcRpc>,
    config: RaftConfig,
    log: Logger,
    inboxSender: Sender<ApplyStep>,
    inbox: Receiver<ApplyStep>,
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
        let (tx, rx) = channel::<ApplyStep>();

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        let io = MemoryIo::new();
        let rpc = TpcRpc::new(config.clone(), tx.clone(), nodes.clone(), log.new(o!()));
        let raft = RaftHandle::new(config.clone(), tx.clone(), io, rpc, logger, nodes.clone());

        RaftServer {
            raft,
            config,
            log,
            inboxSender: tx,
            inbox: rx,
        }
    }

    /// Start the server and the state machine. Raft is driven every 100 milliseconds.
    pub fn start(self, run_for: Option<Duration>) -> RaftHandle<MemoryIo, TpcRpc> {
        self.listen();
        self.serve();

        let mut timeout = Duration::from_millis(100);
        let mut t = Instant::now();
        let mut raft = self.raft.apply(ApplyStep(Command::Start, None)).unwrap();

        let now = Instant::now();

        loop {
            if let Some(run_for) = run_for {
                if now.elapsed() > run_for {
                    break;
                }
            }

            match self.inbox.recv_timeout(timeout) {
                Ok(step) => {
                    raft = raft.apply(step).unwrap();
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return raft,
            }

            let d = t.elapsed();
            t = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(100);
                raft = raft.apply(ApplyStep(Command::Tick, None)).unwrap();
            } else {
                timeout -= d;
            }
        }

        raft
    }

    fn serve(&self) {
        let address = ([127, 0, 0, 1], 3000).into();
        info!(self.log, "serving"; "address" => &address);
        let server = Server::bind(&address)
            .serve(|| {
                service_fn_ok(move |req: Request<Body>| {
                    Response::new(Body::from("Hello World!"))
                })
            })
            .map_err(|e| eprintln!("server error: {}", e));

        thread::spawn(move || { rt::run(server); });
    }

    fn listen(&self) {
        let address = format!("{}:{}", self.config.ip, self.config.port);
        info!(self.log, "listening"; "address" => &address);

        let listener = TcpListener::bind(&address).unwrap();
        let tx = self.inboxSender.clone();
        let log = self.log.new(o!());

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let reader = BufReader::new(&stream);
                        let msg: Message = match serde_json::from_reader(reader) {
                            Ok(msg) => msg,
                            Err(_e) => {
                                info!(log, "Client disconnected."; "address" => format!("{:?}", stream.peer_addr().unwrap()));
                                continue;
                            }
                        };

                        info!(log, ""; "message" => format!("{:?}", msg));
                        let step = match msg {
                            Message::AddNodeRequest(socket_addr) => {
                                ApplyStep(Command::AddNode(socket_addr), None)
                            }
                            Message::AppendRequest(req) => {
                                ApplyStep(Command::AppendEntries {
                                    term: req.term,
                                    leader_id: 0,
                                    entries: vec![],
                                }, None)
                            }
                            Message::AppendResponse(res) => {
                                ApplyStep(Command::AppendResponse {
                                    node_id: res.header.node_id,
                                    term: res.term,
                                    index: res.last_log,
                                }, None)
                            }
                            Message::VoteRequest(req) => {
                                ApplyStep(Command::VoteRequest {
                                    term: req.term,
                                    candidate_id: req.candidate_id,
                                    last_term: req.last_term,
                                    last_index: req.last_index,
                                }, None)
                            }
                            _ => ApplyStep(Command::Noop, None),
                        };
                        tx.send(step).expect("Channel should be open");
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
