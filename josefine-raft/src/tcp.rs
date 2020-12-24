use crate::error::Result;
use crate::raft::{Node, NodeId};
use crate::rpc::{Address, Message};
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

async fn receive_task(listener: TcpListener, in_tx: UnboundedSender<Message>) -> Result<()> {
    let mut s = stream::ListenerStream(listener);
    while let Some((s, _addr)) = s.next().await.transpose()? {
        println!("{:?}", s);
        let peer_in_tx = in_tx.clone();
        tokio::spawn(async move {
            match stream_messages(s, peer_in_tx).await {
                Ok(()) => { println!("!") }
                Err(_) => { println!("err") }
            }
        });
    }

    Ok(())
}

async fn stream_messages(stream: TcpStream, in_tx: UnboundedSender<Message>) -> Result<()> {
    let length_delimited = FramedRead::new(stream, LengthDelimitedCodec::new());
    let mut stream = tokio_serde::SymmetricallyFramed::new(
        length_delimited,
        tokio_serde::formats::SymmetricalJson::default(),
    );
    while let Some(message) = stream.try_next().await? {
        in_tx.send(message)?;
    }
    Ok(())
}

async fn send_task(
    id: NodeId,
    nodes: Vec<Node>,
    out_rx: UnboundedReceiver<Message>,
) -> Result<()> {
    let mut node_txs: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::new();

    for (node) in nodes.iter() {
        let (tx, rx) = mpsc::channel::<Message>(1000);
        node_txs.insert(node.id, tx);
        tokio::spawn(connect_and_send(*node, rx));
    }

    let mut s = stream::UnboundedReceiverStream(out_rx);
    while let Some(mut message) = s.next().await {
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
                None => {} //error!("Received outbound message for unknown peer {}", id),
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
                match send_messages(socket, &mut out_rx).await {
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
    let mut stream = tokio_serde::SymmetricallyFramed::new(
        length_delimited,
        tokio_serde::formats::SymmetricalJson::default(),
    );

    let mut s = stream::ReceiverStream(out_rx);
    while let Some(message) = s.next().await {
        stream.send(message).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Command;
    use crate::rpc::Address;
    use bytes::{Bytes, BytesMut, Buf, BufMut};
    use futures::SinkExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_util::codec::FramedWrite;

    #[tokio::test]
    async fn read_message() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:8084").await?;
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(receive_task(listener, tx));
        let mut stream = TcpStream::connect("127.0.0.1:8084").await?;
        let out_msg = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);

        let mut frame = FramedWrite::new(stream, LengthDelimitedCodec::new());
        frame
            .send(Bytes::from(serde_json::to_string(&out_msg)?))
            .await?;

        match rx.recv().await {
            Some(in_msg) => assert_eq!(out_msg, in_msg),
            _ => panic!(),
        }

        Ok(())
    }

    use super::*;
    use futures::StreamExt;
    use tokio_util::codec::FramedRead;
    use std::thread::Thread;

    #[tokio::test]
    async fn send_message() -> Result<()> {
        let mut listener = TcpListener::bind("127.0.0.1:8080").await?;
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(send_task(
            1,
            vec![Node {
                id: 2,
                addr: "127.0.0.1:8080".parse()?,
            }],
            rx,
        ));

        let out_msg = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);
        let out_msg2 = Message::new(1, Address::Peer(1), Address::Peer(2), Command::Tick);
        tx.send(out_msg)?;

        let mut stream = stream::ListenerStream(listener);
        let (mut stream, _addr) = stream.next().await.unwrap()?;
        let mut frame = FramedRead::new(stream, LengthDelimitedCodec::new());
        match frame.next().await {
            Some(Ok(mut bytes)) => {
                let in_msg = serde_json::from_slice(&mut bytes.as_mut())?;
                assert_eq!(out_msg2, in_msg);
            }
            _ => panic!(),
        };

        Ok(())
    }
}

mod stream {
    use crate::rpc::Message;
    use futures::task::{Context, Poll};
    use std::net::SocketAddr;
    use std::pin::Pin;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc::{UnboundedReceiver, Receiver};
    use tokio_stream::Stream;
    pub struct ReceiverStream<'a, T>(pub &'a mut Receiver<T>);

    impl <'a> Stream for ReceiverStream<'a, Message> {
        type Item = Message;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    pub struct UnboundedReceiverStream<T>(pub UnboundedReceiver<T>);

    impl Stream for UnboundedReceiverStream<Message> {
        type Item = Message;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    pub struct ListenerStream(pub TcpListener);

    impl Stream for ListenerStream {
        type Item = std::io::Result<(TcpStream, SocketAddr)>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let stream = unsafe { Pin::map_unchecked_mut(self, |x| &mut x.0) };

            stream.poll_accept(cx).map(|x| Some(x))
        }
    }
}
