extern crate clap;
extern crate josefine_raft;
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use clap::App;
use clap::Arg;

use josefine_raft::config::RaftConfig;
use josefine_raft::server::RaftServer;

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

    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name(config_path)).expect("Could not read configuration file")
        .merge(config::Environment::with_prefix("JOSEFINE")).expect("Could not read environment variables");

    let config: RaftConfig = settings.try_into().expect("Could not create configuration");

    println!("{:?}", config);
    let server = RaftServer::new(config);
    server.start();
}

