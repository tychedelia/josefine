use std::net::SocketAddr;

use crate::error::Result;
use futures::FutureExt;
use tokio::net::TcpListener;

use crate::broker::tcp;

use kafka_protocol::messages::*;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::broker::state::Store;
use crate::raft::client::RaftClient;

use crate::broker::broker::Broker;
use crate::broker::config::BrokerConfig;

pub struct Server {
    address: SocketAddr,
    config: BrokerConfig,
}

impl Server {
    pub fn new(config: BrokerConfig) -> Self {
        let address = SocketAddr::new(config.ip, config.port);
        Server { address, config }
    }

    pub async fn run(
        self,
        client: RaftClient,
        store: Store,
        shutdown: (
            tokio::sync::broadcast::Sender<()>,
            tokio::sync::broadcast::Receiver<()>,
        ),
    ) -> Result<()> {
        tracing::info!("starting broker");
        let listener = TcpListener::bind(self.address).await?;
        let (in_tx, out_tx) = tokio::sync::mpsc::unbounded_channel();
        let (task, tcp_receiver) =
            tcp::receive_task(listener, in_tx, shutdown.0.subscribe()).remote_handle();
        tokio::spawn(task);

        let ctrl = Broker::new(store, client, self.config);
        let (task, handle_messages) =
            handle_messages(ctrl, out_tx, shutdown.0.subscribe()).remote_handle();
        tokio::spawn(task);

        let (_, _) = tokio::try_join!(tcp_receiver, handle_messages)?;
        Ok(())
    }
}

async fn handle_messages(
    ctrl: Broker,
    mut out_tx: UnboundedReceiver<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,

            Some((msg, cb)) = out_tx.recv() => {
                let res = ctrl.handle_request(msg).await?;
                cb.send(res).unwrap();
            }
        }
    }

    Ok(())
}
