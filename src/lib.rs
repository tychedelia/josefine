use josefine_raft::JosefineRaft;
use josefine_broker::JosefineBroker;
use josefine_core::error::{Result, JosefineError};
use futures::FutureExt;
use josefine_raft::client::RaftClient;
use sled::Db;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref DB: Db = {
        sled::open(tempfile::tempdir().unwrap().into_path()).unwrap()
    };
}

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P) -> Result<()> {
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RaftClient::new(client_tx);
    let broker = JosefineBroker::new(&DB);
    let (task, broker) = broker.run(client).remote_handle();
    tokio::spawn(task);
    let raft = JosefineRaft::with_config(config_path);
    let (task, raft) = raft.run(josefine_broker::fsm::JosefineFsm::new(&DB), client_rx).remote_handle();
    tokio::spawn(task);
    let (_, _) = tokio::try_join!(broker, raft)?;
    Ok(())
}