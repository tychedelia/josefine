use crate::raft::{NodeMap, NodeId};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::rpc::Message;
use tokio::net::TcpListener;
use crate::error::Result;
use crate::tcp::send::TcpSendTask;
use slog::Logger;

struct TcpServer {
    tcp_bind_addr: String,
    nodes: NodeMap,
    logger: Logger,
}

impl TcpServer {
    pub fn new(logger: Logger, nodes: NodeMap, tcp_bind_addr: String) -> Self {
        return TcpServer {
            tcp_bind_addr,
            nodes,
            logger,
        };
    }
}
