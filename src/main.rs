extern crate clap;
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

fn main() {
    let matches = App::new("Josefine")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .value_name("PORT"))
        .arg(Arg::with_name("node")
            .long("node")
            .multiple(true)
            .value_name("ADDRESS"))
        .get_matches();


    let config = match matches.value_of("port") {
        Some(port) => Config {
            port: port.parse().expect("Could not parse port to integer."),
            ..Default::default()
        },
        None => Config::default(),
    };

    let mut server = RaftServer::new(config);

    match matches.values_of("node") {
        Some(nodes) => {
            for node in nodes {
                let parts: Vec<&str> = node.split(":").collect();
                let ip: IpAddr = parts[0].parse().expect("Could not parse ip address.");
                let port: u32 = parts[1].parse().expect("Could not parse port to integer.");

                let node = Node {
                    id: 0,
                    ip,
                    port,
                };

                server.raft.add_node_to_cluster(node);
            }
        }
        None => {}
    };

    server.start();
}

