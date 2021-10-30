

#[tokio::main]
pub async fn main() {
    let mut path = std::env::current_dir().unwrap();
    path.push("examples/single-node/single-node.tom");
    let shutdown = tokio::sync::broadcast::channel(1);
    josefine::josefine(path.as_path(), shutdown).await.unwrap();
}
