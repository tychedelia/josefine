extern crate core;

mod entry;
mod log;
mod index;
mod segment;
mod server;
mod partition;

fn main() {
    println!("Hello, world!");
    let broker = server::Broker::new(0, String::from(""), String::from(""));
    let server = server::Server::new(String::from("localhost:3000"), broker);
    server.start();
}
