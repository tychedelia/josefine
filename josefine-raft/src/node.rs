use std::io;
use std::net::SocketAddr;

use actix::{Actor, ActorContext, actors::{
    resolver::Connect,
    resolver::Resolver
}, AsyncContext, Context, ContextFutureSpawner, fut::ActorFuture, fut::WrapFuture, Handler, Message, Recipient, registry::SystemService, Running, StreamHandler, Supervised};
use actix::io::{FramedWrite, WriteHandler};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use slog::Logger;
use tokio::codec::{FramedRead, LinesCodec};
use tokio::io::{AsyncRead, WriteHalf};
use tokio::net::TcpStream;

use crate::error::RaftError;
use crate::rpc::RpcMessage;

impl Message for RpcMessage {
    type Result = Result<(), RaftError>;
}

/// NodeActor represents another Raft node. Messages recieved will be forwarded
/// directly to the other Raft instance via TCP.
///
/// In this sense, NodeActor is the RPC layer for interacting with the cluster.
///
/// This actor will continuously attempt to connect to the host address, with
/// an exponential backoff.
pub struct NodeActor {
    addr: SocketAddr,
    logger: Logger,
    raft: Recipient<RpcMessage>,
    backoff: ExponentialBackoff,
    writer: Option<FramedWrite<WriteHalf<TcpStream>, LinesCodec>>
}


impl NodeActor {
    /// Creates a new actor instance. Must be provided a host address and an actor address
    /// of the Raft instance it can call back to.
    pub fn new(addr: SocketAddr, logger: Logger, raft: Recipient<RpcMessage>) -> NodeActor {
        // Backoff controlls attempts to reconnect to the given host address
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;

        NodeActor {
            addr,
            logger,
            raft,
            backoff,
            writer: None,
        }
    }
}

impl WriteHandler<io::Error> for NodeActor {
    fn error(&mut self, _err: io::Error, _: &mut Self::Context) -> Running {
        // If we have an error, stop and get restarted
        error!(self.logger, "Error writing");
        Running::Stop
    }
}

impl Supervised for NodeActor {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        // no special action needed when restarting
        info!(self.logger, "Restarting")
    }
}

impl Actor for NodeActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {

        // get tcp client
        Resolver::from_registry()
            .send(Connect::host(self.addr.to_string()))
            .into_actor(self) // transform stream into actor
            .map(|res, mut act, ctx| match res {
                Ok(stream) => {
                    info!(act.logger, "Connected"; "addr" => act.addr.to_string());

                    // get read half and write half of stream
                    let (r, w) = stream.split();

                    let line_reader = FramedRead::new(r, LinesCodec::new());
                    let line_writer = FramedWrite::new(w, LinesCodec::new(), ctx);

                    // write side handles messages sent to us by other service level actors in the system
                    act.writer = Some(line_writer);

                    // read side is called by actix, we add a handler
                    Context::add_stream(ctx, line_reader);

                    act.backoff.reset();
                }
                Err(_err) => {
                    error!(act.logger, "Could not connect"; "addr" => act.addr.to_string());
                    if let Some(timeout) = act.backoff.next_backoff() {
                        Context::run_later(ctx, timeout, |_, ctx| Context::stop(ctx));
                    }
                }
            })
            .map_err(|_err, act, ctx| {
                error!(act.logger, "Could not connect"; "addr" => act.addr.to_string());
                if let Some(timeout) = act.backoff.next_backoff() {
                    Context::run_later(ctx, timeout, |_, ctx| Context::stop(ctx));
                }
            })
            .wait(ctx);
    }
}

impl Handler<RpcMessage> for NodeActor {
    type Result = Result<(), RaftError>;

    fn handle(&mut self, msg: RpcMessage, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(w) = &mut self.writer {
            // Write the message directly to the stream
            w.write(serde_json::to_string(&msg).unwrap() + "\n");
            return Ok(())
        }

        Err(RaftError::MessageError { error_msg: format!("Could not write to {:?}", self.addr) })
    }
}

// Read a line from the tcp stream
impl StreamHandler<String, std::io::Error> for NodeActor {
    fn handle(&mut self, line: String, _ctx: &mut Self::Context) {
        // try to deserialize message
        // TODO(jcm): switch from JSON
        trace!(self.logger, "TCP read"; "line" => &line);
        let message: RpcMessage = serde_json::from_str(&line).unwrap();

        self.raft.try_send(message).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix::{System, Arbiter};
    use std::time::Duration;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::ptr::eq;

    #[test]
    fn read() {
        struct Tester {
            did_pass: bool
        }

        impl Actor for Tester {
            type Context = Context<Self>;

            fn started(&mut self, ctx: &mut Self::Context) {
                ctx.run_later(Duration::from_secs(10), |_, _| {
                    System::current().stop();
                });
            }
        }

        impl Handler<RpcMessage> for Tester {
            type Result = Result<(), RaftError>;

            fn handle(&mut self, msg: RpcMessage, ctx: &mut Self::Context) -> Self::Result {
                Ok(())
            }
        }

        let sys = System::new("test");

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let tester = Arbiter::start(move |_| Tester { did_pass: false });
        Arbiter::start(move |_| NodeActor::new(addr, crate::logger::get_root_logger().new(o!()), tester.recipient()));



        sys.run();
    }
}
