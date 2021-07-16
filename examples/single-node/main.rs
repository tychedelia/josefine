use josefine;

#[tokio::main]
pub async fn main() {
    let mut path = std::env::current_dir().unwrap();
    path.push("examples/single-node/single-node.tom");
    josefine::josefine(path.as_path()).await.unwrap();
}
