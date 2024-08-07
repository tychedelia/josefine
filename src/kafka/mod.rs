use kafka_protocol::messages::{RequestHeader, RequestKind, ResponseKind};
use std::net::SocketAddr;

use crate::Shutdown;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

pub mod codec;
pub mod error;
mod tcp;

#[derive(Debug)]
pub struct KafkaClient {
    stream: TcpStream,
}

#[derive(Debug)]
pub struct ConnectedKafkaClient {
    tx: UnboundedSender<(RequestHeader, RequestKind, oneshot::Sender<ResponseKind>)>,
}

impl ConnectedKafkaClient {
    #[tracing::instrument]
    pub async fn send(
        &self,
        header: RequestHeader,
        req: RequestKind,
    ) -> anyhow::Result<ResponseKind> {
        tracing::trace!(?header, ?req, "send client request");
        let (cb_tx, cb_rx) = tokio::sync::oneshot::channel();
        self.tx.send((header, req, cb_tx))?;
        let res = cb_rx.await?;
        tracing::trace!(?res, "receive client response");
        Ok(res)
    }
}

impl KafkaClient {
    pub async fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(&addr).await?;
        Ok(KafkaClient { stream })
    }

    pub async fn connect(self, shutdown: Shutdown) -> anyhow::Result<ConnectedKafkaClient> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(crate::kafka::tcp::send_messages(self.stream, rx, shutdown));
        Ok(ConnectedKafkaClient { tx })
    }
}
