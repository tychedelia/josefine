use std::path::{Path, PathBuf};
use tracing_subscriber::layer::SubscriberExt;
use clap::Parser;

use josefine::util::Shutdown;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("tokio::task::waker=off".parse().unwrap()),
        )
        .with(fmt::Layer::new().pretty().with_writer(std::io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    ctrlc::set_handler(move || {
        tracing::info!("shut down");
        s.shutdown()
    })
    .unwrap();

    let args = Args::parse();

    josefine::josefine(&args.config, shutdown).await
}
