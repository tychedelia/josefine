use josefine_core::error::Result;
use slog::Logger;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc::UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub async fn receive_task(
    log: Logger,
    listener: TcpListener,
    in_tx: UnboundedSender<String>,
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
    stream: TcpStream,
    in_tx: UnboundedSender<String>,
) -> Result<()> {
    let length_delimited = FramedRead::new(stream, LengthDelimitedCodec::new());
    let mut stream = tokio_serde::SymmetricallyFramed::new(
        length_delimited,
        tokio_serde::formats::SymmetricalBincode::default(),
    );

    while let Some(message) = stream.try_next().await? {
        info!(log, "receive message"; "msg" => format!("{:?}", message));
        in_tx.send(message).unwrap();
    }
    Ok(())
}
