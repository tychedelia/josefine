use slog::Drain;
use slog::Logger;

use crate::config::RaftConfig;

lazy_static! {
    pub static ref LOGGER: Logger = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    };
}

pub fn get_root_logger() -> &'static LOGGER {
    &LOGGER
}

pub fn get_instance_logger(logger: &Logger, config: &RaftConfig) -> Logger {
    logger.new(o!("id" => config.id))
}
