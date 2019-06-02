use actix::{Supervised, Actor, Context, Recipient, Arbiter, StreamHandler, Handler, Message};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::Stream;
use slog::Logger;
use backoff::ExponentialBackoff;
use crate::rpc::RpcMessage;
use tokio::codec::{LinesCodec, FramedRead};
use tokio::io::ReadHalf;
use tokio::io::AsyncRead;
use actix::AsyncContext;
use actix::ActorContext;
use core::mem;
use backoff::backoff::Backoff;
use std::sync::{Mutex, Arc};

pub struct TcpListenerActor {
    addr: SocketAddr,
    logger: Logger,
    backoff: ExponentialBackoff,
    raft: Recipient<RpcMessage>,
}

impl TcpListenerActor {
    pub fn new(addr: SocketAddr, logger: Logger, raft: Recipient<RpcMessage>) -> TcpListenerActor {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;
        TcpListenerActor {
            addr,
            logger,
            backoff,
            raft,
        }
    }
}

struct TcpConnect(TcpStream, SocketAddr);

impl Message for TcpConnect {
    type Result = ();
}

impl Actor for TcpListenerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(self.logger, "Listening"; "port" => format!("{:?}", self.addr));

        match TcpListener::bind(&self.addr) {
            Ok(listener) => {
                ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(|st| {
                    let addr = st.peer_addr().unwrap();
                    TcpConnect(st, addr)
                }));
            },
            Err(_err) => {
                error!(self.logger, "Could bind to address");
                if let Some(timeout) = self.backoff.next_backoff() {
                    Context::run_later(ctx, timeout, |_, ctx| Context::stop(ctx));
                }
            },
        };
    }
}

impl Handler<TcpConnect> for TcpListenerActor {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, _: &mut Context<Self>) {
        let logger = self.logger.new(o!());
        let raft = self.raft.clone();
        let (r, w) = msg.0.split();
        let line_reader = FramedRead::new(r, LinesCodec::new());
        Arbiter::start(move |_| {
            TcpReaderActor::new(logger, raft, line_reader)
        });
    }
}


impl Supervised for TcpListenerActor {

}

struct TcpReaderActor {
    logger: Logger,
    raft: Recipient<RpcMessage>,
    reader: Option<FramedRead<ReadHalf<TcpStream>, LinesCodec>>,
}

impl TcpReaderActor {
    pub fn new(logger: Logger, raft: Recipient<RpcMessage>, reader: FramedRead<ReadHalf<TcpStream>, LinesCodec>) -> TcpReaderActor {
        TcpReaderActor {
            logger,
            raft,
            reader: Some(reader),
        }
    }
}

impl Actor for TcpReaderActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let line_reader = mem::replace(&mut self.reader, None).unwrap();
        Context::add_stream(ctx, line_reader);
    }
}

impl StreamHandler<String, std::io::Error> for TcpReaderActor {
    fn handle(&mut self, line: String, _ctx: &mut Self::Context) {
        if line.is_empty() {
            return
        }

        trace!(self.logger, "TCP read"; "line" => &line);
        let message: RpcMessage = serde_json::from_str(&line).expect(&line);
        self.raft.try_send(message).unwrap();
    }
}
