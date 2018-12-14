extern crate core;

pub mod entry;
pub mod log;
pub mod index;
pub mod segment;
pub mod server;
pub mod partition;
pub mod raft {
    pub mod raft;
    pub mod election;
    pub mod follower;
    pub mod candidate;
    pub mod leader;
    pub mod config;
}

fn main() {
    println!("Hello, world!");
}

