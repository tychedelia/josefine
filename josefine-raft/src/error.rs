use crate::rpc::Message;
use snafu::Snafu;
use tokio::task::JoinError;


pub type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug, Snafu)]
pub enum RaftError {
    #[snafu(display("Cannot configure {}: {}", file_path, error_msg))]
    ConfigError {
        file_path: String,
        error_msg: String,
    },
    #[snafu(display("Error sending message {}", error_msg))]
    ApplyError {
        error_msg: String,
    },
    #[snafu(display("Error sending message {}", error_msg))]
    MessageError {
        error_msg: String,
    },
}

impl From<std::io::Error> for RaftError {
    fn from(err: std::io::Error) -> Self {
        return RaftError::MessageError {
            error_msg: err.to_string(),
        };
    }
}

impl From<tokio::sync::mpsc::error::SendError<Message>> for RaftError {
    fn from(_: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        unimplemented!()
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
            error_msg: err.to_string()
        }
    }
}