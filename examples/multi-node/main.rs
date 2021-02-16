//! A multi-node cluster that shares a single tokio runtime.
use josefine;

#[tokio::main]
pub async fn main() {
    let path = std::env::current_dir().unwrap();
    let mut p1 = path.clone();
    p1.push("examples/multi-node/node-1.tom");
    let f1 = josefine::josefine(p1.as_path());
    let mut p2 = path.clone();
    p2.push("examples/multi-node/node-2.tom");
    let f2 = josefine::josefine(p2.as_path());
    let mut p3 = path.clone();
    p3.push("examples/multi-node/node-3.tom");
    let f3 = josefine::josefine(p3.as_path());

    let (_, _, _ ) = tokio::try_join!(f1, f2, f3).unwrap();
}
