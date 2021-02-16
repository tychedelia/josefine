use crate::config::RaftConfig;
use crate::error::{RaftError, Result};
use crate::logger::get_root_logger;
use crate::raft::{Apply, Command, RaftHandle};
use crate::rpc::{Address, Message};
use crate::tcp;
use futures::FutureExt;
use slog::Logger;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

/// step duration
const TICK: Duration = Duration::from_millis(100);

pub struct Server {
    config: RaftConfig,
    log: Logger,
}

impl Server {
    pub fn new(config: RaftConfig) -> Self {
        Server {
            config,
            log: get_root_logger().new(o!()),
        }
    }

    pub async fn run(self, duration: Option<Duration>) -> Result<RaftHandle> {
        info!(self.log, "Using config"; "config" => format!("{:?}", self.config));

        // shutdown broadcaster
        let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);

        // tcp receive
        let socket_addr = SocketAddr::new(self.config.ip, self.config.port);
        let listener = TcpListener::bind(socket_addr).await?;
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_receiver) = tcp::receive_task(
            self.log.new(o!()),
            shutdown_tx.subscribe(),
            listener,
            tcp_in_tx,
        )
        .remote_handle();
        tokio::spawn(task);

        // tcp send
        let (tcp_out_tx, tcp_out_rx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_sender) = tcp::send_task(
            self.log.new(o!()),
            shutdown_tx.subscribe(),
            self.config.id,
            self.config.clone().nodes,
            tcp_out_rx,
        )
        .remote_handle();
        tokio::spawn(task);

        // main event loop
        let raft = RaftHandle::new(self.log.new(o!()), self.config, rpc_tx.clone());
        let (task, event_loop) = event_loop(
            self.log.new(o!()),
            shutdown_tx.subscribe(),
            raft,
            tcp_out_tx,
            rpc_rx,
            tcp_in_rx,
        )
        .remote_handle();
        tokio::spawn(task);

        if let Some(duration) = duration {
            tokio::time::sleep(duration).await;
            shutdown_tx.send(())?;
        }

        let (_, _, raft) = tokio::try_join!(tcp_receiver, tcp_sender, event_loop)?;
        Ok(raft)
    }
}

async fn event_loop(
    log: Logger,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
    mut raft: RaftHandle,
    tcp_tx: UnboundedSender<Message>,
    mut rpc_rx: UnboundedReceiver<Message>,
    mut tcp_rx: UnboundedReceiver<Message>,
) -> Result<RaftHandle> {
    let mut step_interval = tokio::time::interval(TICK);
    info!(log, "starting event loop");

    loop {
        tokio::select! {
        // shutdown
        _ = shutdown.recv() => break,
        // tick state machine
        _ = step_interval.tick() => raft = raft.apply(Command::Tick)?,
        // intra-cluster communication
        Some(msg) = tcp_rx.recv() => raft = raft.apply(msg.command)?,
        // outgoing messages from raft
        Some(msg) = rpc_rx.recv() => {
            match msg {
                Message { to: Address::Peer(_), .. } => tcp_tx.send(msg)?,
                Message { to: Address::Peers, ..  } => tcp_tx.send(msg)?,
                _ => return Err(RaftError::Internal { error_msg: format!("Unexpected message {:?}", msg) }),
            }

            }
        }
        // TODO: client connections
    }

    Ok(raft)
}

#[cfg(test)]
mod tests {
    use crate::config::RaftConfig;
    use crate::error::Result;
    use crate::logger::get_root_logger;
    use crate::raft::RaftHandle;

    use futures_util::core_reexport::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn event_loop() -> Result<()> {
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let raft = RaftHandle::new(
            get_root_logger().new(o!()),
            RaftConfig::default(),
            rpc_tx.clone(),
        );

        let (_tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel();
        let (tcp_out_tx, _tcp_out_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);
        let event_loop = super::event_loop(
            get_root_logger().new(o!()),
            shutdown_tx.subscribe(),
            raft,
            tcp_out_tx,
            rpc_rx,
            tcp_in_rx,
        );
        let raft = tokio::spawn(event_loop);
        std::thread::sleep(Duration::from_secs(2));
        shutdown_tx.send(())?;

        let raft = tokio::try_join!(raft)?.0?;
        if let RaftHandle::Leader(_raft) = raft {
        } else {
            panic!("was not elected leader");
        }
        Ok(())
    }
}
