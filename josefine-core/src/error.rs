use sled::transaction::TransactionError;

pub type Result<T> = std::result::Result<T, JosefineError>;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, PartialOrd)]
pub enum JosefineError {
    ConfigError {
        file_path: String,
        error_msg: String,
    },
    ApplyError {
        error_msg: String,
    },
    MessageError {
        error_msg: String,
    },
    Internal {
        error_msg: String,
    },
}

impl From<std::io::Error> for JosefineError {
    fn from(err: std::io::Error) -> Self {
        JosefineError::MessageError {
            error_msg: err.to_string(),
        }
    }
}

impl From<serde_json::error::Error> for JosefineError {
    fn from(_: serde_json::error::Error) -> Self {
        unimplemented!()
    }
}

impl From<std::net::AddrParseError> for JosefineError {
    fn from(_: std::net::AddrParseError) -> Self {
        unimplemented!()
    }
}

impl From<tokio::task::JoinError> for JosefineError {
    fn from(err: tokio::task::JoinError) -> Self {
        JosefineError::ApplyError {
            error_msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::oneshot::error::TryRecvError> for JosefineError {
    fn from(_: tokio::sync::oneshot::error::TryRecvError) -> Self {
        unimplemented!()
    }
}

impl From<tokio::sync::broadcast::error::SendError<()>> for JosefineError {
    fn from(_: tokio::sync::broadcast::error::SendError<()>) -> Self {
        unimplemented!()
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for JosefineError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        unimplemented!()
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for JosefineError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        unimplemented!()
    }
}

impl From<Box<bincode::ErrorKind>> for JosefineError {
    fn from(_: Box<bincode::ErrorKind>) -> Self {
        unimplemented!()
    }
}

impl From<TransactionError<JosefineError>> for JosefineError {
    fn from(_: TransactionError<JosefineError>) -> Self {
        todo!()
    }
}
