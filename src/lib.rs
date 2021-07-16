pub mod config;
pub mod error;
pub mod logger;
pub mod broker;
pub mod kafka;
pub mod raft;

use futures::FutureExt;
use crate::broker::JosefineBroker;
use crate::error::{Result};
use crate::raft::client::RaftClient;
use sled::Db;

use crate::raft::JosefineRaft;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref DB: Db = sled::open(tempfile::tempdir().unwrap().into_path()).unwrap();
}

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P) -> Result<()> {
    let config = config::config(config_path);

    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RaftClient::new(client_tx);
    let broker = JosefineBroker::with_config(config.broker);
    let (task, broker) = broker.run(client, &DB).remote_handle();
    tokio::spawn(task);

    let raft = JosefineRaft::with_config(config.raft);
    let (task, raft) = raft
        .run(crate::broker::fsm::JosefineFsm::new(&DB), client_rx)
        .remote_handle();
    tokio::spawn(task);
    let (_, _) = tokio::try_join!(broker, raft)?;
    Ok(())
}
