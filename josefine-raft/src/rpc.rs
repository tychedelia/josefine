use crate::raft::{Command, Entry, LogIndex, NodeId, Term};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
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
            RpcMessage::Append { term, leader_id, entries, prev_log_index, prev_log_term, .. } => Command::AppendEntries {
                term,
                leader_id,
                entries,
                prev_log_index,
                prev_log_term,
            },
            RpcMessage::RespondAppend(term, node_id, success) => Command::AppendResponse {
                node_id,
                term,
                success,
                index: 0,
            },
        }
    }
}
