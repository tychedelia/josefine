use std::fmt::{Display, Formatter};
use std::error::Error;
use kafka_protocol::protocol::{DecodeError, EncodeError};
use kafka_protocol::protocol::buf::NotEnoughBytesError;

#[derive(Debug)]
pub enum ErrorKind {
    DecodeError,
    EncodeError,
    UnsupportedOperation,
    IoError(std::io::Error)
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::DecodeError => {
                writeln!(f, "Error decoding message")
            },
            ErrorKind::EncodeError => {
                writeln!(f, "Error encoding message")
            },
            ErrorKind::UnsupportedOperation => {
                writeln!(f, "Unsupported API")
            },
            ErrorKind::IoError(err) => {
                writeln!(f, "IoError: {}", err)
            }
        }
    }
}

impl Error for ErrorKind {

}

impl From<std::io::Error> for ErrorKind {
    fn from(err: std::io::Error) -> Self {
        ErrorKind::IoError(err)
    }
}

impl From<DecodeError> for ErrorKind {
    fn from(err: DecodeError) -> Self {
        ErrorKind::DecodeError
    }
}

impl From<EncodeError> for ErrorKind {
    fn from(err: EncodeError) -> Self {
        ErrorKind::EncodeError
    }
}

impl From<()> for ErrorKind {
    fn from(_: ()) -> Self {
        ErrorKind::DecodeError
    }
}

impl From<NotEnoughBytesError> for ErrorKind {
    fn from(err: NotEnoughBytesError) -> Self {
        ErrorKind::DecodeError
    }
}