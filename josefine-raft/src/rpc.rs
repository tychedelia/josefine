use crate::raft::{Term, NodeId, Command, LogIndex, Entry};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    Ping(Term, NodeId),
    RequestVote(Term, NodeId, Term, LogIndex),
    RespondVote(Term, NodeId, bool),
    Heartbeat(Term, NodeId),
    Append { term: Term, leader_id: NodeId, prev_log_index: LogIndex, prev_log_term: Term, entries: Vec<Entry>, leader_commit: LogIndex },
    RespondAppend(Term, NodeId, bool),
    Tick,
}

impl From<RpcMessage> for Command {
    fn from(msg: RpcMessage) -> Self {
        match msg {
            RpcMessage::Heartbeat(term, leader_id) => Command::Heartbeat { term, leader_id },
            RpcMessage::Ping(term, id) => Command::Ping(term, id),
            RpcMessage::RespondVote(term, from, granted) => Command::VoteResponse {
                term,
                from,
                granted,
            },
            RpcMessage::RequestVote(term, candidate_id, last_term, last_index) => Command::VoteRequest {
                term,
                candidate_id,
                last_term,
                last_index,
            },
            RpcMessage::Tick => Command::Tick,
            RpcMessage::Append { term, leader_id, entries, .. } => Command::AppendEntries {
                term,
                leader_id,
                entries
            },
            RpcMessage::RespondAppend(term, node_id, success) => Command::AppendResponse {
                node_id,
                term,
                index: 0,
            },
            _ => Command::Noop
        }
    }
}