use snafu::Snafu;

pub type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug, Snafu)]
pub enum RaftError {
    #[snafu(display("Cannot configure {}: {}", file_path, error_msg))]
    ConfigError {
        file_path: String,
        error_msg: String,
    },
    ApplyError,
    #[snafu(display("Error sending message {}", error_msg))]
    MessageError {
        error_msg: String,
    },
}
