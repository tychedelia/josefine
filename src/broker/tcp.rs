use crate::error::{JosefineError, Result};
use crate::kafka::codec::KafkaServerCodec;
use futures::SinkExt;
use kafka_protocol::messages::{RequestKind, ResponseHeader, ResponseKind};
use slog::Logger;
use tokio::sync::oneshot;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedSender,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

pub async fn receive_task(
    log: Logger,
    listener: TcpListener,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,

            Ok((s, addr)) = listener.accept() => {
                let log = log.new(o!("addr" => format!("{:?}", addr)));
                debug!(log, "peer connected");
                let peer_in_tx = in_tx.clone();
                tokio::spawn(async move {
                    match stream_messages(log.clone(), s, peer_in_tx).await {
                        Ok(()) => { debug!(log, "peer disconnected") }
                        Err(err) => { error!(log, "error reading from peer"; "err" => format!("{:?}", err)) }
                    }
                });
            }
        }
    }

    Ok(())
}

async fn stream_messages(
    _log: Logger,
    mut stream: TcpStream,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
) -> Result<()> {
    let (r, w) = stream.split();
    let mut stream_in = FramedRead::new(r, KafkaServerCodec::new());
    let mut stream_out = FramedWrite::new(w, KafkaServerCodec::new());
    while let Some((header, message)) =
        stream_in
            .try_next()
            .await
            .map_err(|_err| JosefineError::MessageError {
                error_msg: "broke".to_string(),
            })?
    {
        let (cb_tx, cb_rx) = oneshot::channel();
        in_tx
            .send((message, cb_tx))
            .map_err(|e| JosefineError::MessageError {
                error_msg: format!("{:?}", e),
            })?;
        let res = cb_rx.await.map_err(|e| JosefineError::MessageError {
            error_msg: format!("{:?}", e),
        })?;
        let version = header.request_api_version;
        let correlation_id = header.correlation_id;
        let header = ResponseHeader {
            correlation_id,
            ..Default::default()
        };
        stream_out
            .send((version, header, res))
            .await
            .map_err(|e| JosefineError::MessageError {
                error_msg: format!("{:?}", e),
            })?;
    }
    Ok(())
}
