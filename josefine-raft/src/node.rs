use std::net::SocketAddr;

use actix::{Actor, ActorContext, actors::{
    resolver::Connect,
    resolver::Resolver
}, AsyncContext, Context, ContextFutureSpawner, fut::ActorFuture, fut::WrapFuture, registry::SystemService, StreamHandler, Recipient, Message, Handler, Supervised, Running};
use serde::private::de::Content;
use slog::Logger;
use tokio::codec::{Decoder, FramedRead, LinesCodec};
use tokio::io::{AsyncRead, WriteHalf};
use std::io::Write;
use std::time::Duration;
use std::{thread, io};
use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use actix::io::{FramedWrite, WriteHandler};
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    Ping
}

impl Message for RpcMessage {
    type Result = ();
}

struct NodeActor {
    addr: SocketAddr,
    log: Logger,
    raft: Recipient<RpcMessage>,
    backoff: ExponentialBackoff,
    write: Option<FramedWrite<WriteHalf<TcpStream>, LinesCodec>>
}


impl NodeActor {
    pub fn new(addr: SocketAddr, log: Logger, raft: Recipient<RpcMessage>) -> NodeActor {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;
        NodeActor {
            addr,
            log,
            raft,
            backoff,
            write: None,
        }
    }
}

impl WriteHandler<io::Error> for NodeActor {
    fn error(&mut self, err: io::Error, _: &mut Self::Context) -> Running {
        error!(self.log, "Error writing");
        Running::Stop
    }
}

impl Supervised for NodeActor {
    fn restarting(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Restarting...")
    }
}

impl Actor for NodeActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.log, "Starting...");

        Resolver::from_registry()
            .send(Connect::host(self.addr.to_string()))
            .into_actor(self)
            .map(|res, mut act, ctx| match res {
                Ok(stream) => {
                    info!(act.log, "Connected"; "addr" => act.addr.to_string());

                    let (r, mut w) = stream.split();
                    let line_reader = FramedRead::new(r, LinesCodec::new());
                    let line_writer = FramedWrite::new(w, LinesCodec::new(), ctx);
                    act.write = Some(line_writer);

                    Context::add_stream(ctx, line_reader);

                    act.backoff.reset();
                }
                Err(err) => {
                    error!(act.log, "Could not connect"; "addr" => act.addr.to_string());
                    if let Some(timeout) = act.backoff.next_backoff() {
                        Context::run_later(ctx, timeout, |_, ctx| Context::stop(ctx));
                    }
                }
            })
            .map_err(|err, act, ctx| {
                error!(act.log, "Could not connect"; "addr" => act.addr.to_string());
                if let Some(timeout) = act.backoff.next_backoff() {
                    Context::run_later(ctx, timeout, |_, ctx| Context::stop(ctx));
                }
            })
            .wait(ctx);
    }
}

impl StreamHandler<String, std::io::Error> for NodeActor {
    fn handle(&mut self, line: String, _ctx: &mut Self::Context) {
        let message: RpcMessage = serde_json::from_str(&line).unwrap();
        self.raft.do_send(message).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::log::get_root_logger;
    use crate::node::{NodeActor, Raft};
    use std::time::Duration;
    use std::thread;

    #[test]
    fn it_starts() {
        let system = actix::System::new("node-test");
        let raft = actix::Arbiter::start(|_| Raft {});
        let node = actix::Supervisor::start(|_|  super::NodeActor::new("127.0.0.1:8080".parse().unwrap(), get_root_logger(), raft.recipient()));

//        system.run();
    }
}