use futures::Stream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio_serde::{Framed, SymmetricallyFramed};
use tokio_util::codec::{LengthDelimitedCodec, FramedRead};
use bytes::Bytes;

use crate::error::Result;
use crate::rpc::Message;
use tokio::stream::StreamExt;

struct TcpReceiveTask {}

impl TcpReceiveTask {
    pub async fn new(mut listener: TcpListener, in_tx: UnboundedSender<Message>) -> Result<()> {
        println!("new");
        while let Some(socket) = listener.try_next().await? {
            println!("connection {:?}", socket);
            let peer_in_tx = in_tx.clone();
            tokio::spawn(async move {
                match Self::receive_messages(socket, peer_in_tx).await {
                    Ok(()) => {}, // debug!("Raft peer {} disconnected", peer),
                    Err(err) => println!("error: {:?}", err.to_string()),
                };
            });
        }
        Ok(())
    }

    async fn receive_messages(socket: TcpStream, in_tx: UnboundedSender<Message>, ) -> Result<()> {
        // identify frames with a header indicating length
        let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());
        let mut stream = tokio_serde::SymmetricallyFramed::new( length_delimited, tokio_serde::formats::SymmetricalJson::default());
        while let Some(message) = stream.try_next().await? {
            in_tx.send(message)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use crate::rpc::Address;
    use crate::raft::Command;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::FramedWrite;
    use futures::SinkExt;

    #[tokio::test]
    async fn read_message() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(TcpReceiveTask::new(listener, tx));

        let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
        let out_msg = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);

        let mut frame = FramedWrite::new(stream, LengthDelimitedCodec::new());
        frame.send(Bytes::from(serde_json::to_string(&out_msg)?)).await?;

        match rx.next().await {
            Some(in_msg) => assert_eq!(out_msg, in_msg),
            _ => panic!(),
        }
        Ok(())
    }
}