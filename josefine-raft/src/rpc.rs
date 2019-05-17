use crate::raft::Term;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    Ping,
    RespondVote(Term, bool)
}