use std::collections::HashMap;
use std::net::SocketAddr;
use kafka_protocol::messages::{RequestHeader, RequestKind, ResponseHeader, ResponseKind};

use tokio::net::{TcpStream};
use tokio::sync::mpsc::{UnboundedSender};
use tokio::sync::oneshot;


pub mod codec;
pub mod error;
pub mod util;
mod tcp;

#[derive(Debug)]
pub struct KafkaClient {
    addr: SocketAddr,
    stream: TcpStream,
    cbs: HashMap<i16, tokio::sync::oneshot::Receiver<(ResponseHeader, ResponseKind)>>
}

#[derive(Debug)]
pub struct ConnectedKafkaClient {
    tx: UnboundedSender<(RequestHeader, RequestKind, oneshot::Sender<ResponseKind>)>,
}

impl ConnectedKafkaClient {
    #[tracing::instrument]
    pub async fn send(&self, header: RequestHeader, req: RequestKind) -> anyhow::Result<ResponseKind> {
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
        Ok(KafkaClient {
            addr,
            stream,
            cbs: Default::default()
        })
    }

    pub async fn connect(self, shutdown: tokio::sync::broadcast::Receiver<()>) -> anyhow::Result<ConnectedKafkaClient> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(crate::kafka::tcp::send_messages(self.stream, rx, shutdown));
        Ok(ConnectedKafkaClient {
            tx
        })
    }
}