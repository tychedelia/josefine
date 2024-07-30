use std::net::SocketAddr;

use anyhow::Result;
use futures::FutureExt;
use tokio::net::TcpListener;

use crate::broker::tcp;

use kafka_protocol::messages::*;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::broker::state::Store;
use crate::raft::client::RaftClient;

use crate::broker::config::BrokerConfig;
use crate::broker::Broker;
use crate::Shutdown;

pub struct Server {
    address: SocketAddr,
    config: BrokerConfig,
}

impl Server {
    pub fn new(config: BrokerConfig) -> Self {
        let address = SocketAddr::new(config.ip, config.port);
        Server { address, config }
    }

    pub async fn run(self, client: RaftClient, store: Store, shutdown: Shutdown) -> Result<()> {
        tracing::info!(
            "broker listening on {}:{}",
            self.config.ip,
            self.config.port
        );
        let listener = TcpListener::bind(self.address).await?;
        let (in_tx, out_tx) = tokio::sync::mpsc::unbounded_channel();
        let (task, tcp_receiver) =
            tcp::receive_task(listener, in_tx, shutdown.clone()).remote_handle();
        tokio::spawn(task);

        let ctrl = Broker::new(store, client, self.config);
        let (task, handle_messages) = handle_messages(ctrl, out_tx, shutdown).remote_handle();
        tokio::spawn(task);

        let (_, _) = tokio::try_join!(tcp_receiver, handle_messages)?;
        Ok(())
    }
}

async fn handle_messages(
    ctrl: Broker,
    mut out_tx: UnboundedReceiver<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: Shutdown,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.wait() => break,

            Some((msg, cb)) = out_tx.recv() => {
                let res = ctrl.handle_request(msg).await?;
                cb.send(res).unwrap();
            }
        }
    }

    Ok(())
}
