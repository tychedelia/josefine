use crate::kafka::codec::KafkaServerCodec;
use anyhow::Result;
use futures::SinkExt;
use kafka_protocol::messages::{RequestKind, ResponseHeader, ResponseKind};

use tokio::sync::oneshot;

use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedSender,
};
use tokio::sync::oneshot::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use crate::Shutdown;

pub async fn receive_task(
    listener: TcpListener,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: Shutdown,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.wait() => break,

            Ok((s, _addr)) = listener.accept() => {
                let peer_in_tx = in_tx.clone();
                tokio::spawn(async move {
                    match stream_messages(s, peer_in_tx).await {
                        Ok(()) => {  }
                        Err(_err) => {  }
                    }
                });
            }
        }
    }

    Ok(())
}

async fn stream_messages(
    mut stream: TcpStream,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
) -> Result<()> {
    let (r, w) = stream.split();
    let mut stream_in = FramedRead::new(r, KafkaServerCodec::new());
    let mut stream_out = FramedWrite::new(w, KafkaServerCodec::new());
    while let Some((header, message)) = stream_in.try_next().await? {
        let (cb_tx, cb_rx) = oneshot::channel();
        in_tx.send((message, cb_tx))?;
        let res = cb_rx.await?;
        let version = header.request_api_version;
        let correlation_id = header.correlation_id;
        let header = ResponseHeader {
            correlation_id,
            ..Default::default()
        };
        stream_out.send((version, header, res)).await?;
    }
    Ok(())
}
