pub mod broker;
pub mod config;
pub mod kafka;
pub mod raft;
pub mod util;

use crate::broker::JosefineBroker;
use crate::config::JosefineConfig;
use crate::raft::client::RaftClient;
use anyhow::Result;
use futures::FutureExt;
use tokio::sync::broadcast::{Receiver, Sender};

use crate::raft::JosefineRaft;
use crate::util::Shutdown;

#[macro_use]
extern crate serde_derive;

pub async fn josefine_with_config(config: JosefineConfig, shutdown: Shutdown) -> Result<()> {
    run(config, shutdown).await?;
    Ok(())
}

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P, shutdown: Shutdown) -> Result<()> {
    let config = config::config(config_path);
    run(config, shutdown).await?;
    Ok(())
}

#[tracing::instrument]
pub async fn run(config: JosefineConfig, shutdown: Shutdown) -> Result<()> {
    tracing::info!("start");
    let db = sled::open(&config.broker.state_file).unwrap();

    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RaftClient::new(client_tx);
    let josefine_broker = JosefineBroker::new(config.broker);
    let broker = broker::state::Store::new(db);
    let (task, b) = josefine_broker
        .run(client, broker.clone(), shutdown.clone())
        .remote_handle();
    tokio::spawn(task);

    let raft = JosefineRaft::new(config.raft);
    let (task, raft) = raft
        .run(
            crate::broker::fsm::JosefineFsm::new(broker),
            client_rx,
            shutdown.clone(),
        )
        .remote_handle();
    tokio::spawn(task);

    let (_, _) = tokio::try_join!(b, raft)?;
    Ok(())
}
