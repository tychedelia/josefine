use tempfile::tempdir;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use crate::broker::command::Controller;
use crate::broker::store::Store;
use crate::error::JosefineError;
use crate::raft::client::RaftClient;
use crate::raft::rpc::{Proposal, Response};

pub(crate) fn new_controller() -> (UnboundedReceiver<(Proposal, Sender<Result<Response, JosefineError>>)>, Controller) {
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    (client_rx, Controller {
        store: Store::new(sled::open(tempdir().unwrap()).unwrap()),
        client: RaftClient::new(client_tx),
        config: Default::default()
    })
}