use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};
use tracing_subscriber::layer::SubscriberExt;

use josefine::util::Shutdown;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;
    let shutdown = setup_shutdown()?;
    let config = get_config();

    josefine::josefine(&config, shutdown).await
}

fn get_config() -> PathBuf {
    let args = Args::parse();
    args.config
}

fn setup_shutdown() -> anyhow::Result<Shutdown> {
    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    ctrlc::set_handler(move || {
        tracing::info!("shut down");
        s.shutdown()
    })
    .context("Unable to set ctrlc handler")?;
    Ok(shutdown)
}

fn setup_tracing() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::DEBUG.into())
                .add_directive("tokio::task::waker=off".parse().unwrap()),
        )
        .with(fmt::Layer::new().compact().with_writer(std::io::stdout));
    tracing::subscriber::set_global_default(subscriber)
        .context("Unable to set a global collector")?;
    Ok(())
}
