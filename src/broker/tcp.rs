use crate::kafka::codec::KafkaServerCodec;
use futures::SinkExt;
use kafka_protocol::messages::{RequestKind, ResponseHeader, ResponseKind};
use anyhow::Result;

use tokio::sync::oneshot;

use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedSender,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

pub async fn receive_task(
    listener: TcpListener,
    in_tx: UnboundedSender<(RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,

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
    while let Some((header, message)) =
        stream_in
            .try_next()
            .await?
    {
        let (cb_tx, cb_rx) = oneshot::channel();
        in_tx.send((message, cb_tx))?;
        let res = cb_rx.await?;
        let version = header.request_api_version;
        let correlation_id = header.correlation_id;
        let header = ResponseHeader {
            correlation_id,
            ..Default::default()
        };
        stream_out
            .send((version, header, res))
            .await?;
    }
    Ok(())
}

// pub struct PeerClient {
//     addr: SocketAddr,
// }
//
// impl PeerClient {
//     pub async fn connect(&self) -> PeerCon {
//         let tcp = TcpStream::connect(&self.addr)
//             .await
//             .expect("tcp connect error");
//         let (tx, rx) = mpsc::unbounded_channel::<(
//             Option<Vec<u8>>,
//             oneshot::Sender<Result<Option<Vec<u8>>>>,
//         )>();
//         let span = tracing::info_span!("tcp_client", addr = %self.addr);
//         tracing::info!(parent: &span, "connected");
//         let task = tokio::spawn(
//             async move {
//                 let mut rx = rx;
//                 let mut tcp = tcp;
//                 while let Some((action, cb)) = rx.recv().await {
//                     match action {
//                         None => {
//                             let mut vec = vec![0; 1024];
//                             match tcp.read(&mut vec).await {
//                                 Ok(n) => {
//                                     vec.truncate(n);
//                                     tracing::trace!(read = n, data = ?vec);
//                                     let _ = cb.send(Ok(Some(vec)));
//                                 }
//                                 Err(e) => {
//                                     tracing::trace!(read_error = %e);
//                                     cb.send(Err(JosefineError::from(e))).unwrap();
//                                     break;
//                                 }
//                             }
//                         }
//                         Some(vec) => match tcp.write_all(&vec).await {
//                             Ok(_) => {
//                                 let _ = cb.send(Ok(None));
//                             }
//                             Err(e) => {
//                                 tracing::trace!(write_error = %e);
//                                 cb.send(Err(JosefineError::from(e))).unwrap();
//                                 break;
//                             }
//                         },
//                     }
//                 }
//             }
//         );
//         PeerCon {
//             addr: self.addr,
//             tx,
//             task,
//         }
//     }
// }
//
// pub struct PeerCon {
//     addr: SocketAddr,
//     tx: mpsc::UnboundedSender<(
//         Option<Vec<u8>>,
//         oneshot::Sender<Result<Option<Vec<u8>>>>,
//     )>,
//     task: JoinHandle<()>,
// }
//
// impl PeerCon {
//     async fn read(&self) -> Vec<u8> {
//         self.try_read()
//             .await
//             .unwrap_or_else(|e| panic!("TcpConn(addr={}) read() error: {:?}", self.addr, e))
//     }
//
//     async fn read_timeout(&self, timeout: Duration) -> Vec<u8> {
//         tokio::time::timeout(timeout, self.read())
//             .await
//             .unwrap_or_else(|e| panic!("TcpConn(addr={}) read() error: {}", self.addr, e))
//     }
//
//     async fn try_read(&self) -> Result<Vec<u8>> {
//         async {
//             let (tx, rx) = oneshot::channel();
//             let _ = self.tx.send((None, tx));
//             let res = rx.await.expect("tcp read dropped");
//             res.map(|opt| opt.unwrap())
//         }
//             .await
//     }
//
//     async fn write(&self, req: (RequestHeader, RequestKind)) {
//         async {
//             let (tx, rx) = oneshot::channel();
//             let _ = self.tx.send((Some(buf), tx));
//             rx.await
//                 .map(|rsp| assert!(rsp.unwrap().is_none()))
//                 .expect("tcp write dropped")
//         }
//             .await
//     }
//
//     // pub async fn send(&self, req: (RequestHeader, RequestKind)) -> Result<(ResponseHeader, ResponseKind)> {
//     //
//     // }
//
//     pub async fn shutdown(self) {
//         let Self { tx, task, .. } = self;
//         drop(tx);
//         task.await.unwrap()
//     }
// }
