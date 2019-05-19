use crate::raft::{Term, NodeId, Command, LogIndex};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    Ping(Term, NodeId),
    RequestVote(Term, NodeId, Term, LogIndex),
    RespondVote(Term, NodeId, bool),
    Tick,
}

impl From<RpcMessage> for Command {
    fn from(msg: RpcMessage) -> Self {
        match msg {
            RpcMessage::Ping(term, id) => Command::Ping(term, id),
            RpcMessage::RespondVote(term, candidate_id, granted) => Command::VoteResponse {
                term,
                candidate_id,
                granted,
            },
            RpcMessage::RequestVote(term, candidate_id, last_term, last_index) => Command::VoteRequest {
                term,
                candidate_id,
                last_term,
                last_index,
            },
            RpcMessage::Tick => Command::Tick,
            _ => Command::Noop
        }
    }
}