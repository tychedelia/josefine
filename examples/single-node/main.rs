use josefine::util::Shutdown;

#[tokio::main]
pub async fn main() {
    let mut path = std::env::current_dir().unwrap();
    path.push("examples/single-node/single-node.tom");
    let shutdown = Shutdown::new();
    josefine::josefine(path.as_path(), shutdown).await.unwrap();
}
