extern crate josefine_raft;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use std::thread;

use slog::Drain;

use josefine_raft::config::Config;
use josefine_raft::server::RaftServer;

fn main() {


    let config = Config::default();
    let raft = RaftServer::new(config);

    let config2 = Config {
        port: 6668,
        ..Default::default()
    };

    let raft2 = RaftServer::new(config2);

    thread::spawn(|| {
        raft.start();
    });

    thread::spawn(|| {
        raft2.start();
    });

    loop {

    }
}

