use snafu::Snafu;
use actix::prelude::SendError;
use std::any::Any;

#[derive(Debug, Snafu)]
pub enum RaftError {
    #[snafu(display("Cannot configure {}: {}", file_path, error_msg))]
    ConfigError { file_path: String, error_msg: String },
    ApplyError,
    #[snafu(display("Error sending message {}", error_msg))]
    MessageError { error_msg: String },
}

impl <T> From<SendError<T>> for RaftError {
    fn from(err: SendError<T>) -> Self {
        RaftError::MessageError { error_msg: err.to_string() }
    }
}
