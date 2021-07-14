use josefine_core::error::{Result, JosefineError};
use slog::Logger;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc::UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec, FramedWrite};
use kafka_protocol::messages::{RequestKind, ResponseKind, ResponseHeader};
use tokio::sync::oneshot;
use josefine_kafka::codec::KafkaServerCodec;
use futures::SinkExt;

pub async fn receive_task(
    log: Logger,
    listener: TcpListener,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
) -> Result<()> {
    loop {
        tokio::select! {
            Ok((s, addr)) = listener.accept() => {
                let log = log.new(o!("addr" => format!("{:?}", addr)));
                info!(log, "peer connected");
                let peer_in_tx = in_tx.clone();
                tokio::spawn(async move {
                    match stream_messages(log.clone(), s, peer_in_tx).await {
                        Ok(()) => { info!(log, "peer disconnected") }
                        Err(err) => { error!(log, "error reading from peer"; "err" => format!("{:?}", err)) }
                    }
                });
            }
        }
    }
}


async fn stream_messages(
    log: Logger,
    mut stream: TcpStream,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
) -> Result<()> {
    let (r, w) = stream.split();
    let mut stream_in = FramedRead::new(r, KafkaServerCodec::new());
    let mut stream_out = FramedWrite::new(w, KafkaServerCodec::new());
    while let Some((header, message)) = stream_in.try_next().await.map_err(|err| {
        JosefineError::MessageError { error_msg: "broke".to_string() }
    })? {
        let (cb_tx, mut cb_rx) = oneshot::channel();
        in_tx.send((message, cb_tx)).map_err(|e| JosefineError::MessageError { error_msg: format!("{:?}", e)})?;
        let res = cb_rx.await.map_err(|e| JosefineError::MessageError { error_msg: format!("{:?}", e) })?;
        let version = header.request_api_version;
        let correlation_id = header.correlation_id;
        let mut header = ResponseHeader::default();
        header.correlation_id = correlation_id;
        stream_out.send((version, header, res)).await.map_err(|e| JosefineError::MessageError { error_msg: format!("{:?}", e) })?;
    }
    Ok(())
}