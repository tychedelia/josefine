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


    let config_map = settings.try_into::<HashMap<String, String>>().expect("Could not parse config");

    let mut config = Config::default();

    if config_map.contains_key("id") {
        config.id = config_map["id"].parse().expect(format!("Could not parse id {} to integer", config_map["id"]).as_str());
    }
    if config_map.contains_key("ip") {
        config.ip = config_map["ip"].parse().expect(format!("Could not parse ip address {}", config_map["ip"]).as_str());
    }
    if config_map.contains_key("port") {
        config.port = config_map["port"].parse().expect(format!("Could not parse port {} into integer", config_map["port"]).as_str())
    }

    let mut server = RaftServer::new(config);
    server.start();
}

