pub mod broker;
pub mod config;
pub mod error;
pub mod kafka;
pub mod raft;

use crate::broker::JosefineBroker;
use crate::error::Result;
use crate::raft::client::RaftClient;
use futures::FutureExt;


use crate::raft::JosefineRaft;

#[macro_use]
extern crate serde_derive;

pub async fn josefine<P: AsRef<std::path::Path>>(
    config_path: P,
    shutdown: (
        tokio::sync::broadcast::Sender<()>,
        tokio::sync::broadcast::Receiver<()>,
    ),
) -> Result<()> {
    let config = config::config(config_path);
    let db = sled::open(&config.broker.file).unwrap();

    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RaftClient::new(client_tx);
    let josefine_broker = JosefineBroker::with_config(config.broker);
    let broker = crate::broker::store::Store::new(db);
    let (task, b) = josefine_broker
        .run(
            client,
            broker.clone(),
            (shutdown.0.clone(), shutdown.0.subscribe()),
        )
        .remote_handle();
    tokio::spawn(task);

    let raft = JosefineRaft::with_config(config.raft);
    let (task, raft) = raft
        .run(
            crate::broker::fsm::JosefineFsm::new(broker),
            client_rx,
            (shutdown.0.clone(), shutdown.0.subscribe()),
        )
        .remote_handle();
    tokio::spawn(task);
    let (_, _) = tokio::try_join!(b, raft)?;
    Ok(())
}
