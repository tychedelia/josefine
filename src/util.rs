use anyhow::anyhow;
use futures_util::TryFutureExt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct Shutdown(
    tokio::sync::broadcast::Sender<()>,
    tokio::sync::broadcast::Receiver<()>,
);

impl Shutdown {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        Shutdown(tx, rx)
    }

    pub fn shutdown(&self) {
        self.0.send(()).expect("shutdown channel closed");
    }

    pub async fn wait(&mut self) -> anyhow::Result<()> {
        let shutdown = self.1.recv().await?;
        Ok(shutdown)
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Shutdown(self.0.clone(), self.0.subscribe())
    }
}
