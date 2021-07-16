use crate::rpc::{Proposal, Response};
use josefine_core::error::{JosefineError, Result};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

pub struct RaftClient {
    request_tx: UnboundedSender<(Proposal, oneshot::Sender<Result<Response>>)>,
}

impl RaftClient {
    /// Creates a new Raft client.
    pub fn new(request_tx: UnboundedSender<(Proposal, oneshot::Sender<Result<Response>>)>) -> Self {
        Self { request_tx }
    }

    /// Executes a request against the Raft cluster.
    async fn request(&self, request: Proposal) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }

    /// Proposes a state transition to the Raft state machine.
    pub async fn propose(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        Ok(self.request(Proposal::new(command)).await?.get())
    }
}
