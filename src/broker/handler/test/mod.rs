use crate::broker::state::Store;
use crate::broker::{Broker, Replicas};
use crate::raft::client::RaftClient;
use crate::raft::rpc::{Proposal, Response, ResponseError};
use tempfile::tempdir;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;

pub(crate) fn new_broker() -> (
    UnboundedReceiver<(Proposal, Sender<std::result::Result<Response, ResponseError>>)>,
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
