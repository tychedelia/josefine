#[macro_use]
extern crate serde_derive;
extern crate serde;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod follower;
mod candidate;
mod leader;
mod election;
pub mod raft;
pub mod config;
mod progress;
mod rpc;
pub mod server;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
