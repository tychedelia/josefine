use crate::broker::{Broker, Replicas};
use crate::broker::state::Store;
use crate::error::JosefineError;
use crate::raft::client::RaftClient;
use crate::raft::rpc::{Proposal, Response};
use tempfile::tempdir;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;

pub(crate) fn new_broker() -> (
    UnboundedReceiver<(Proposal, Sender<Result<Response, JosefineError>>)>,
    Broker,
) {
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    (
        client_rx,
        Broker {
            store: Store::new(sled::open(tempdir().unwrap()).unwrap()),
            client: RaftClient::new(client_tx),
            config: Default::default(),
            replicas: Replicas::new(),
        },
    )
}
