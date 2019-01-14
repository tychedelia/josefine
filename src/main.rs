extern crate clap;
extern crate config;
extern crate josefine_raft;
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use std::net::IpAddr;

use clap::App;
use clap::Arg;

use josefine_raft::config::Config;
use josefine_raft::raft::Node;
use josefine_raft::server::RaftServer;
use std::collections::HashMap;
use config::ConfigError;

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
        .merge(config::Environment::with_prefix("JOSEFINE")).unwrap();

    let mut config = Config::default();

    match settings.get("id") {
        Ok(id) =>  config.id = id,
        Err(ConfigError::NotFound(_)) => {},
        Err(e) => panic!(format!("{}", e)),
    }
    match settings.get("ip") {
        Ok(ip) =>  config.ip = ip,
        Err(ConfigError::NotFound(_)) => {},
        Err(e) => panic!(format!("{}", e)),
    }
    match settings.get("port") {
        Ok(port) =>  config.port = port,
        Err(ConfigError::NotFound(_)) => {},
        Err(e) => panic!(format!("{}", e)),
    }

    let mut server = RaftServer::new(config);
    server.start();
}

