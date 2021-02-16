use josefine_raft::JosefineRaft;

pub async fn josefine<P: AsRef<std::path::Path>>(config_path: P) {
    let raft = JosefineRaft::with_config(config_path);
    raft.run().await.unwrap();
}
