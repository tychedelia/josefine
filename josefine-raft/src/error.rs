use crate::{fsm, rpc::Message};
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug, Snafu)]
pub enum RaftError {
    #[snafu(display("Cannot configure {}: {}", file_path, error_msg))]
    ConfigError {
        file_path: String,
        error_msg: String,
    },
    #[snafu(display("Error sending message {}", error_msg))]
    ApplyError { error_msg: String },
    #[snafu(display("Error sending message {}", error_msg))]
    MessageError { error_msg: String },
    #[snafu(display("Unknown internal error {}", error_msg))]
    Internal { error_msg: String },
}

impl From<std::io::Error> for RaftError {
    fn from(err: std::io::Error) -> Self {
        RaftError::MessageError {
            error_msg: err.to_string(),
        }
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

impl From<serde_json::error::Error> for RaftError {
    fn from(_: serde_json::error::Error) -> Self {
        unimplemented!()
    }
}

impl From<std::net::AddrParseError> for RaftError {
    fn from(_: std::net::AddrParseError) -> Self {
        unimplemented!()
    }
}

impl From<tokio::task::JoinError> for RaftError {
    fn from(err: tokio::task::JoinError) -> Self {
        RaftError::ApplyError {
            error_msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::oneshot::error::TryRecvError> for RaftError {
    fn from(_: tokio::sync::oneshot::error::TryRecvError) -> Self {
        unimplemented!()
    }
}

impl From<tokio::sync::broadcast::error::SendError<()>> for RaftError {
    fn from(_: tokio::sync::broadcast::error::SendError<()>) -> Self {
        unimplemented!()
    }
}
