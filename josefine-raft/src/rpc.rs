use crate::raft::{Term, NodeId, Command};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    Ping(Term, NodeId),
    RespondVote(Term, bool)
}

impl From<RpcMessage> for Command {
    fn from(msg: RpcMessage) -> Self {
        match msg {
            RpcMessage::Ping(term, id) => Command::Ping(term, id),
            _ => Command::Noop
        }
    }
}