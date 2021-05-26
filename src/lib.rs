use josefine_raft::JosefineRaft;

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P) -> Result<(), Box<dyn std::error::Error>>{
    let raft = JosefineRaft::with_config(config_path);
    let (_, client_rx) = tokio::sync::mpsc::unbounded_channel(); 
    raft.run(josefine_core::fsm::JosefineFsm, client_rx).await?;
    Ok(())
}
