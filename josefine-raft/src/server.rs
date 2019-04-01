use std::collections::HashMap;
use std::io::BufReader;
use std::io::Read;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use hyper::{Body, Request, Response, rt, Server};
use hyper::rt::run;
use hyper::service::service_fn_ok;
use slog::*;
use tokio::prelude::future::Future;
use tokio::prelude::stream::Stream;

use crate::config::RaftConfig;
use crate::io::Io;
use crate::io::MemoryIo;
use crate::log;
use crate::raft::{Apply, ApplyStep, RaftContainer};
use crate::raft::Command;
use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::NodeMap;
use crate::raft::RaftHandle;
use crate::rpc::{InfoRequest, Message};
use crate::rpc::Message::Ping;
use crate::rpc::TpcRpc;

/// A server implementation that wraps the Raft state machine and handles connection with other nodes via a TPC
/// RPC implementation.
///
/// The server handles wiring up the state machine and driving it forward at a fixed interval.
pub struct RaftServer {
    config: RaftConfig,
    log: Logger,
    tx: Sender<ApplyStep>,
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
    pub fn new(config: RaftConfig, log: Logger, tx: Sender<ApplyStep>) -> RaftServer {
        RaftServer {
            config,
            log,
            tx,
        }
    }

    pub fn listen(&self) -> thread::JoinHandle<()> {

        let address = format!("{}:{}", self.config.ip, self.config.port);
        info!(self.log, "listening"; "address" => &address);

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
        })
    }
}


#[cfg(test)]
mod tests {
    use std::{mem, thread};
    use std::sync::mpsc::channel;
    use std::time::Duration;

    use crate::config::RaftConfig;
    use crate::log;
    use crate::raft::{ApplyStep, RaftHandle};
    use crate::server::RaftServer;

    #[test]
    fn it_runs() {
        let election_timeout_max = 1000; // TODO: Expose constants better
        let config = RaftConfig { run_for:  Some(Duration::from_millis(election_timeout_max)), ..RaftConfig::default() };
        let (tx, rx) = channel::<ApplyStep>();
        let server = RaftServer::new(config, log::get_root_logger(), tx);

        let t = server.listen();
    }
}
