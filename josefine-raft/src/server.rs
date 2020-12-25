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
use slog::Logger;

/// step duration
const TICK: Duration = Duration::from_millis(100);

struct Server {
    config: RaftConfig,
    raft: RaftHandle,
    log: Logger,
}

impl Server {
    pub async fn new(config: RaftConfig) -> Self {
        let (rpc_tx, _rpc_rx) = mpsc::unbounded_channel();
        let raft = RaftHandle::new(config.clone(), get_root_logger().new(o!()), rpc_tx);
        Server {
            config,
            raft,
            log: get_root_logger().new(o!()),
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
        let (task, eventloop) = event_loop(self.log.new(o!()), self.raft,  rpc_rx, tcp_in_rx).remote_handle();
        tokio::spawn(task);

        tokio::try_join!(tcp_receiver, tcp_sender, eventloop)?;
        Ok(())
    }
}

async fn event_loop(
    mut log: Logger,
    mut raft: RaftHandle,
    mut rpc_rx: UnboundedReceiver<Message>,
    mut tcp_rx: UnboundedReceiver<Message>,
) -> Result<RaftHandle> {
    let mut step_interval = tokio::time::interval(TICK);
    info!(log, "starting event loop");

    loop {
        tokio::select! {
                // tick state machine
                _ = step_interval.tick() => raft = raft.apply(Command::Tick)?,
                // intra-cluster communication
                Some(msg) = tcp_rx.recv() => raft = raft.apply(msg.command)?,
                // outgoing messages from raft
                Some(msg) = rpc_rx.recv() => {
                    match msg {
                        Message { command: Command::Exit, .. } => break,
                //         Message { to: Address::Peer(_), .. } => (),
                        _ => panic!("incomplete match")
                    }
                }
                // TODO: client connections
            }
    }

    Ok(raft)
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use crate::raft::{RaftHandle, Command};
    use crate::config::RaftConfig;
    use crate::logger::get_root_logger;
    use crate::error::Result;
    use crate::rpc::{Message, Address};
    use futures_util::core_reexport::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn event_loop() -> Result<()> {
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let raft = RaftHandle::new(RaftConfig::default(), get_root_logger().new(o!()), rpc_tx.clone());

        let (tcp_tx, tcp_rx) = mpsc::unbounded_channel();
        let event_loop = super::event_loop(get_root_logger().new(o!()), raft, rpc_rx, tcp_rx);
        let raft = tokio::spawn(event_loop);
        std::thread::sleep(Duration::from_secs(2));

        rpc_tx.send(Message::new(0, Address::Local, Address::Local, Command::Exit))?;
        let raft = tokio::try_join!(raft)?.0?;
        if let RaftHandle::Leader(raft) = raft {

        } else {
            panic!("was not elected leader");
        }
        Ok(())
    }
}
