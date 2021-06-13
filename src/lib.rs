use josefine_raft::JosefineRaft;
use josefine_broker::JosefineBroker;
use josefine_core::error::Result;

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P) -> Result<()> {
    let (client_tx, client_rx) = tokio::sync::mpsc::unbounded_channel();
    let broker = JosefineBroker::new();
    broker.run(client_tx);
    let raft = JosefineRaft::with_config(config_path);
    raft.run(josefine_broker::fsm::JosefineFsm, client_rx).await?;
    Ok(())
}
