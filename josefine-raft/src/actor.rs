use actix::{Actor, Context, Handler};
use crate::raft::{RaftHandle};

struct RaftActor {
    raft: RaftHandle
}

impl Actor for RaftActor {
    type Context = Context<Self>;
}