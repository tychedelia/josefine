use crate::config::RaftConfig;
use crate::error::Result;
use crate::logger::get_root_logger;
use crate::raft::{Apply, Command, RaftHandle};
use crate::rpc::{Address, Message};
use crate::tcp;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use futures::FutureExt;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// step duration
const TICK: Duration = Duration::from_millis(100);

struct Server {
    config: RaftConfig,
    raft: RaftHandle,
}

impl Server {
    pub async fn new(config: RaftConfig) -> Self {
        let (rpc_tx, _rpc_rx) = mpsc::unbounded_channel();
        let raft = RaftHandle::new(config.clone(), get_root_logger().new(o!()), rpc_tx);
        Server {
            config,
            raft
        }
    }

    pub async fn run(self) -> Result<()> {
        let socket_addr = SocketAddr::new(self.config.ip, self.config.port);
        let listener = TcpListener::bind(socket_addr).await?;
        let (rpc_tx_, rpc_rx) = mpsc::unbounded_channel();

        let (tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel::<Message>();
        let (tcp_out_tx, tcp_out_rx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_receiver) = tcp::receive_task(listener, tcp_in_tx).remote_handle();
        tokio::spawn(task);
        let (task, tcp_sender) = tcp::send_task(self.config.id, self.config.clone().nodes, tcp_out_rx).remote_handle();
        tokio::spawn(task);
        let (task, eventloop) = event_loop(self.raft,  rpc_rx, tcp_in_rx).remote_handle();
        tokio::spawn(task);

        tokio::try_join!(tcp_receiver, tcp_sender, eventloop)?;
        Ok(())
    }
}

async fn event_loop(
    mut raft: RaftHandle,
    mut rpc_rx: UnboundedReceiver<Message>,
    mut tcp_rx: UnboundedReceiver<Message>,
) -> Result<()> {
    let mut step_interval = tokio::time::interval(TICK);

    loop {
        tokio::select! {
                // tick state machine
                _ = step_interval.tick() => raft = raft.apply(Command::Tick)?,
                // intra-cluster communication
                Some(msg) = tcp_rx.recv() => raft = raft.apply(msg.command)?,
                // outgoing messages from raft
                Some(msg) = rpc_rx.recv() => {
                    match msg {
                        Message { to: Address::Peer(_), .. } => (),
                        _ => panic!()
                    }
                }
                // TODO: client connections
            }
    }
}
