use crate::raft::rpc::{Address, Message, Proposal, Response, ResponseError};
use crate::raft::{
    config::RaftConfig,
    fsm::{self},
};
use crate::raft::{tcp, ClientRequestId};
use crate::raft::{Apply, Command, RaftHandle};
use anyhow::Result;
use futures::FutureExt;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;
use tokio::{
    net::TcpListener,
    sync::{mpsc::unbounded_channel, oneshot},
};
use uuid::Uuid;

/// step duration
const TICK: Duration = Duration::from_millis(100);

pub struct Server {
    config: RaftConfig,
}

pub struct ServerRunOpts<T: 'static + fsm::Fsm> {
    pub duration: Option<Duration>,
    pub fsm: T,
    pub client_rx: UnboundedReceiver<(
        Proposal,
        oneshot::Sender<std::result::Result<Response, ResponseError>>,
    )>,
    pub shutdown: (
        tokio::sync::broadcast::Sender<()>,
        tokio::sync::broadcast::Receiver<()>,
    ),
}

impl Server {
    pub fn new(config: RaftConfig) -> Self {
        Server { config }
    }

    pub async fn run<T: 'static + fsm::Fsm>(
        self,
        run_opts: ServerRunOpts<T>,
    ) -> Result<RaftHandle> {
        let ServerRunOpts {
            duration,
            fsm,
            client_rx,
            shutdown,
        } = run_opts;
        let (shutdown_tx, _shutdown_rx) = shutdown;

        // tcp receive
        let socket_addr = SocketAddr::new(self.config.ip, self.config.port);
        let listener = TcpListener::bind(socket_addr).await?;
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_receiver) =
            tcp::receive_task(shutdown_tx.subscribe(), listener, tcp_in_tx).remote_handle();
        tokio::spawn(task);

        // tcp send
        let (tcp_out_tx, tcp_out_rx) = mpsc::unbounded_channel::<Message>();
        let (task, tcp_sender) = tcp::send_task(
            shutdown_tx.clone(),
            self.config.id,
            self.config.clone().nodes,
            tcp_out_rx,
        )
        .remote_handle();
        tokio::spawn(task);

        // state machine driver
        let (fsm_tx, fsm_rx) = unbounded_channel();
        let driver = fsm::Driver::new(fsm_rx, rpc_tx.clone(), fsm);
        let (task, driver) = driver.run(shutdown_tx.subscribe()).remote_handle();
        tokio::spawn(task);

        // main event loop
        let raft = RaftHandle::new(self.config, rpc_tx.clone(), fsm_tx.clone());
        let (task, event_loop) = event_loop(
            shutdown_tx.subscribe(),
            raft,
            tcp_out_tx,
            rpc_rx,
            tcp_in_rx,
            client_rx,
        )
        .remote_handle();
        tokio::spawn(task);

        if let Some(duration) = duration {
            tokio::time::sleep(duration).await;
            shutdown_tx.send(())?;
        }

        let (raft, _, _, _) = tokio::try_join!(event_loop, tcp_receiver, tcp_sender, driver)?;
        Ok(raft)
    }
}

async fn event_loop(
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
    mut raft: RaftHandle,
    tcp_tx: UnboundedSender<Message>,
    mut rpc_rx: UnboundedReceiver<Message>,
    mut tcp_rx: UnboundedReceiver<Message>,
    mut client_rx: UnboundedReceiver<(
        Proposal,
        oneshot::Sender<std::result::Result<Response, ResponseError>>,
    )>,
) -> Result<RaftHandle> {
    let mut step_interval = tokio::time::interval(TICK);
    let mut requests = HashMap::<
        ClientRequestId,
        oneshot::Sender<std::result::Result<Response, ResponseError>>,
    >::new();

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
                    Message { to: Address::Local, .. } => raft = raft.apply(msg.command)?,
                    Message { to: Address::Client, command: Command::ClientResponse { id, res }, .. } => {
                        match requests.remove(&id) {
                            Some(tx) => tx.send(res).expect("the channel was dropped"),
                            None => {
                                panic!("could not find response tx");
                            }
                        };
                    },
                    _ => return Err(anyhow::anyhow!("unexpected message {:?}", msg)),
                }
            },
            // incoming messages from clients
            Some((proposal, res)) = client_rx.recv() => {
                let id = Uuid::new_v4();
                requests.insert(id, res);
                raft = raft.apply(Command::ClientRequest { id, proposal, client_address: Address::Client })?;
            },
        }
    }

    Ok(raft)
}

#[cfg(test)]
mod tests {
    use crate::raft::RaftConfig;
    use crate::raft::RaftHandle;
    use anyhow::Result;

    use std::time::Duration;
    use tokio::sync::mpsc::{self, unbounded_channel};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn event_loop() -> Result<()> {
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (fsm_tx, _fsm_rx) = unbounded_channel();
        let raft = RaftHandle::new(RaftConfig::default(), rpc_tx.clone(), fsm_tx.clone());

        let (_tcp_in_tx, tcp_in_rx) = mpsc::unbounded_channel();
        let (tcp_out_tx, _tcp_out_rx) = mpsc::unbounded_channel();
        let (_client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
        let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);
        let event_loop = super::event_loop(
            shutdown_tx.subscribe(),
            raft,
            tcp_out_tx,
            rpc_rx,
            tcp_in_rx,
            client_rx,
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
