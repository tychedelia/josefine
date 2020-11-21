use crate::raft::{NodeMap, NodeId};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::rpc::Message;
use tokio::net::TcpListener;
use crate::error::Result;
use crate::tcp::send::TcpSendTask;

struct TcpServer {
    tcp_bind_addr: String,
    nodes: NodeMap,
}

impl TcpServer {
    pub fn new(nodes: NodeMap, tcp_bind_addr: String) -> Self {
        return TcpServer {
            tcp_bind_addr,
            nodes,
        };
    }
}
