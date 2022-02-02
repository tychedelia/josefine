use futures::SinkExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::kafka::codec::KafkaClientCodec;
use kafka_protocol::messages::{RequestHeader, RequestKind, ResponseKind};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use crate::Shutdown;

pub async fn send_messages(
    stream: TcpStream,
    mut rx: UnboundedReceiver<(RequestHeader, RequestKind, oneshot::Sender<ResponseKind>)>,
    mut shutdown: Shutdown,
) -> anyhow::Result<()> {
    let requests: Arc<Mutex<HashMap<i32, RequestHeader>>> = Default::default();
    let (r, w) = stream.into_split();
    let mut stream_in = FramedRead::new(r, KafkaClientCodec::new(requests.clone()));
    let mut stream_out = FramedWrite::new(w, KafkaClientCodec::new(requests.clone()));

    let cbs: Arc<Mutex<HashMap<i32, oneshot::Sender<ResponseKind>>>> = Default::default();
    let cbs1 = cbs.clone();
    let write = tokio::spawn(async move {
        while let Some((header, req, cb)) = rx.recv().await {
            let correlation_id = header.correlation_id;
            stream_out.send((header, req)).await?;
            let mut cbs = cbs1.lock().unwrap();
            cbs.insert(correlation_id, cb);
        }
        anyhow::Result::<_, anyhow::Error>::Ok(())
    });

    let cbs2 = cbs.clone();
    let read = tokio::spawn(async move {
        while let Some((header, res)) = stream_in.try_next().await? {
            let mut cbs = cbs2.lock().unwrap();
            let cb = cbs
                .remove(&header.correlation_id)
                .expect("unknown correlation id");
            cb.send(res).unwrap();
        }
        anyhow::Result::<_, anyhow::Error>::Ok(())
    });

    let shutdown = tokio::spawn(async move {
        shutdown.wait().await?;
        anyhow::Result::<_, anyhow::Error>::Ok(())
    });

    let _ = futures::future::select_all(vec![read, write, shutdown]).await;
    Ok(())
}
