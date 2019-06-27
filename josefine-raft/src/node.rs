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

use crate::rpc::RpcMessage;

impl Message for RpcMessage {
    type Result = ();
}

pub struct NodeActor {
    addr: SocketAddr,
    logger: Logger,
    raft: Recipient<RpcMessage>,
    backoff: ExponentialBackoff,
    writer: Option<FramedWrite<WriteHalf<TcpStream>, LinesCodec>>
}


impl NodeActor {
    pub fn new(addr: SocketAddr, logger: Logger, raft: Recipient<RpcMessage>) -> NodeActor {
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
        error!(self.logger, "Error writing");
        Running::Stop
    }
}

impl Supervised for NodeActor {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        info!(self.logger, "Restarting")
    }
}

impl Actor for NodeActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        Resolver::from_registry()
            .send(Connect::host(self.addr.to_string()))
            .into_actor(self)
            .map(|res, mut act, ctx| match res {
                Ok(stream) => {
                    info!(act.logger, "Connected"; "addr" => act.addr.to_string());

                    let (r, w) = stream.split();
                    let line_reader = FramedRead::new(r, LinesCodec::new());
                    let line_writer = FramedWrite::new(w, LinesCodec::new(), ctx);
                    act.writer = Some(line_writer);

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
    type Result = ();

    fn handle(&mut self, msg: RpcMessage, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(w) = &mut self.writer {
            w.write(serde_json::to_string(&msg).unwrap() + "\n");
        }
    }
}

impl StreamHandler<String, std::io::Error> for NodeActor {
    fn handle(&mut self, line: String, _ctx: &mut Self::Context) {
        trace!(self.logger, "TCP read"; "line" => &line);
        let message: RpcMessage = serde_json::from_str(&line).unwrap();
        self.raft.try_send(message).unwrap();
    }
}
