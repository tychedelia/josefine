use actix::{Actor, Context, Handler};
use crate::raft::{RaftContainer, RaftHandle};

struct RaftActor {
    raft: RaftHandle
}

impl Actor for RaftActor {
    type Context = Context<Self>;
}