use clap::App;
use clap::Arg;

use std::path::Path;
use tracing_subscriber::layer::SubscriberExt;

use tracing_subscriber::{fmt, EnvFilter};
use josefine::util::Shutdown;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::TRACE.into())
                .add_directive("tokio::task::waker=off".parse().unwrap()),
        )
        .with(fmt::Layer::new().pretty().with_writer(std::io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    let matches = App::new("Josefine")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("PATH")
                .required(true)
                .default_value("Config.toml")
                .help("Location of the config file."),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();

    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    ctrlc::set_handler(move || {
        tracing::info!("shut down");
        s.shutdown()
    }).unwrap();

    josefine::josefine(Path::new(&config_path), shutdown).await
}
