extern crate josefine_raft;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate clap;

use std::thread;

use slog::Drain;

use josefine_raft::config::Config;
use josefine_raft::server::RaftServer;
use clap::{Arg, App, SubCommand};


fn main() {
    let matches = App::new("Josefine")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .value_name("PORT"))
        .get_matches();


    let config = match matches.value_of("port") {
        Some(port) => Config {
            port: port.parse().expect("Could not parse port to integer."),
            ..Default::default()
        },
        None => Config::default(),
    };

    let raft = RaftServer::new(config);
    raft.start();
}

