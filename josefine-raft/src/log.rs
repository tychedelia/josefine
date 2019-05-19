use slog::Drain;
use slog::Logger;
use crate::config::RaftConfig;

pub fn get_root_logger(config: RaftConfig) -> Logger {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(plain)
            .build().fuse(), o!()
    );


    logger
}
