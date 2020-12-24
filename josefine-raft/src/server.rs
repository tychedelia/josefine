use crate::config::RaftConfig;
use crate::error::Result;
use crate::logger::get_root_logger;
use crate::raft::{Apply, Command, NodeId, NodeMap, RaftHandle};
use crate::rpc::{Address, Message};
use futures::Stream;
use slog::Logger;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Duration;
use tokio_stream::StreamExt;

/// step duration
const TICK: Duration = Duration::from_millis(100);

struct Server {
    raft: RaftHandle,
}

impl Server {
    pub async fn new(config: RaftConfig) {
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let nodes: NodeMap = config
            .clone()
            .nodes
            .into_iter()
            .map(|node| (node.id, node))
            .collect();
        let handle = RaftHandle::new(config, get_root_logger().new(o!()), nodes, rpc_tx);
    }

    pub async fn run(self) {}

    async fn event_loop(
        mut raft: RaftHandle,
        mut rpc_rx: UnboundedReceiver<Message>,
    ) -> Result<()> {
        let mut step_interval = tokio::time::interval(TICK);
        loop {
            tokio::select! {
                _ = step_interval.tick() => raft = raft.apply(Command::Tick)?,

                Some(msg) = rpc_rx.recv() => {
                    match msg {
                        Message { to: Address::Peer(_), .. } => (),
                        _ => panic!()
                    }
                }
            }
        }
    }
}
