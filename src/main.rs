extern crate clap;
extern crate josefine_raft;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use clap::App;
use clap::Arg;

use josefine_raft::config::RaftConfig;
use josefine_raft::server::RaftServer;
use slog::Drain;
use slog::Logger;

fn main() {
    let matches = App::new("Josefine")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(Arg::with_name("config")
            .long("config")
            .value_name("PATH")
            .required(true)
            .default_value("Config.toml")
            .help("Location of the config file."))
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let config = get_config(config_path);
    let logger = get_logger();

    info!(logger, "Using configuration values"; "config" => format!("{:?}", config));

    let server = RaftServer::new(config, logger);
    server.start();
}

fn get_config(config_path: &str) -> RaftConfig {
    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name(config_path)).expect("Could not read configuration file")
        .merge(config::Environment::with_prefix("JOSEFINE")).expect("Could not read environment variables");

    settings.try_into().expect("Could not create configuration")
}

fn get_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    Logger::root(drain, o!())
}

