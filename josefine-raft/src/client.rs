use tokio::sync::mpsc::UnboundedSender;
use crate::rpc::{Request, Response};
use tokio::sync::oneshot;
use josefine_core::error::{Result, JosefineError};

pub struct RaftClient {
    request_tx: UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>
}

impl RaftClient {
    /// Creates a new Raft client.
    pub fn new(
        request_tx: UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Self {
        Self { request_tx }
    }

    /// Executes a request against the Raft cluster.
    async fn request(&self, request: Request) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }

    /// Proposes a state transition to the Raft state machine.
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Propose(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(JosefineError::Internal { error_msg: format!("Unexpected Raft mutate response {:?}", resp) }),
        }
    }

    /// Queries the Raft state machine.
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Query(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(JosefineError::Internal { error_msg: format!("Unexpected Raft query response {:?}", resp) }),
        }
    }
}