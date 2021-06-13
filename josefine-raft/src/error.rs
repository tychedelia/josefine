use josefine_core::error::JosefineError;
use crate::{fsm, rpc::Message};

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, PartialOrd)]
pub enum RaftError {
    MessageError { error_msg: String },
}

impl From<RaftError> for JosefineError {
    fn from(err: RaftError) -> JosefineError {
        unimplemented!()
    }
}

impl From<tokio::sync::mpsc::error::SendError<Message>> for RaftError {
    fn from(err: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        RaftError::MessageError {
            error_msg: err.to_string(),
        }
    }
}


impl From<tokio::sync::mpsc::error::SendError<fsm::Instruction>> for RaftError {
    fn from(err: tokio::sync::mpsc::error::SendError<fsm::Instruction>) -> Self {
        RaftError::MessageError {
            error_msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::mpsc::error::TrySendError<Message>> for RaftError {
    fn from(_: tokio::sync::mpsc::error::TrySendError<Message>) -> Self {
        unimplemented!()
    }
}
