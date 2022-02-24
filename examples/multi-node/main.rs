//! A multi-node cluster that shares a single tokio runtime.

use josefine::util::Shutdown;


use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
pub async fn main() {
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("tokio::task::waker=off".parse().unwrap()),
        )
        .with(fmt::Layer::new().pretty().with_writer(std::io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    let path = std::env::current_dir().unwrap();
    let shutdown = Shutdown::new();
    let tasks: Vec<_> = (1..4)
        .map(|i| {
            let mut p = path.clone();
            p.push(format!("examples/multi-node/node-{i}.toml"));
            Box::pin(josefine::josefine(p.as_path().to_owned(), shutdown.clone()))
        })
        .collect();

    ctrlc::set_handler(move || shutdown.shutdown()).unwrap();

    let _tasks = futures::future::join_all(tasks).await;
}
