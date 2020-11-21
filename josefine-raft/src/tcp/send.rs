use crate::raft::{Node, NodeId, NodeMap};
use crate::rpc::{Message, Address};
use crate::error::Result;
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio::time::Duration;
use tokio_util::codec::{Framed, LengthDelimitedCodec, FramedWrite};
use futures::sink::SinkExt;
use tokio::stream::StreamExt;
use std::collections::HashMap;
use tokio_serde::SymmetricallyFramed;
use slog::Logger;
use std::sync::Arc;

pub struct TcpSendTask {}

impl TcpSendTask {
    /// Create a new send task for the given nodes.
    ///
    /// * `nodes` - Nodes messages will be sent to.
    pub async fn new(id: NodeId, nodes: Vec<Node>, mut out_rx: UnboundedReceiver<Message>) -> Result<()> {
        let mut node_txs: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::new();

        for (node) in nodes.iter() {
            let (tx, rx) = mpsc::channel::<Message>(1000);
            node_txs.insert(node.id, tx);
            tokio::spawn(Self::connect_and_send(*node, rx));
        }

        while let Some(mut message) = out_rx.next().await {
            if message.from == Address::Local {
                message.from = Address::Peer(id)
            }
            let to = match &message.to {
                Address::Peers => node_txs.keys().cloned().collect(),
                Address::Peer(peer) => vec![*peer],
                addr => {
                    // error!("Received outbound message for non-TCP address {:?}", addr);
                    continue;
                }
            };
            for id in to {
                match node_txs.get_mut(&id) {
                    Some(tx) => match tx.try_send(message.clone()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // debug!("Full send buffer for peer {}, discarding message", id)
                        }
                        Err(error) => return Err(error.into()),
                    },
                    None => {}//error!("Received outbound message for unknown peer {}", id),
                }
            }
        }
        Ok(())
    }

    /// Create a new send task for a given node.
    ///
    /// * `node` - The node which messages will be sent to.
    /// * `out_rx` - The channel messages to send are written to.
    async fn connect_and_send(node: Node, mut out_rx: Receiver<Message>) -> Result<()> {
        loop {
            match TcpStream::connect(node.addr).await {
                Ok(socket) => {
                    match Self::send_messages(socket, &mut out_rx).await {
                        Ok(()) => break Ok(()),
                        Err(err) => {} //debug!("Failed sending to Raft peer {}: {}", node.addr, err),
                    }
                }
                Err(err) => {} //debug!("Failed connecting to Raft peer {}: {}", node.addr, err),
            }
            // TODO: use back-off
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }

    /// Write messages to socket in a loop.
    ///
    /// * `socket` - The TCP socket messages will be written to.
    /// * `out_rx` - The channel from which to receive new messages to write.
    async fn send_messages(socket: TcpStream, out_rx: &mut mpsc::Receiver<Message>) -> Result<()> {
        // identify frames with a header indicating length
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
        let mut stream = tokio_serde::SymmetricallyFramed::new(length_delimited, tokio_serde::formats::SymmetricalJson::default());

        while let Some(message) = out_rx.next().await {
            stream.send(message).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Command;
    use tokio_util::codec::FramedRead;
    use futures::StreamExt;
    use bytes::Buf;

    #[tokio::test]
    async fn send_message() -> Result<()> {
        let mut listener = TcpListener::bind("127.0.0.1:8080").await?;
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(TcpSendTask::new(1, vec![Node { id: 2, addr: "127.0.0.1:8080".parse()? }], rx));

        let out_msg = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);
        let out_msg2 = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);
        tx.send(out_msg)?;

        let stream = listener.next().await.unwrap()?;
        let mut frame = FramedRead::new(stream, LengthDelimitedCodec::new());
        match frame.next().await {
            Some(Ok(bytes)) => {
                let in_msg = serde_json::from_slice(bytes.bytes())?;
                assert_eq!(out_msg2, in_msg);
            }
            _ => panic!()
        };

        Ok(())
    }
}
