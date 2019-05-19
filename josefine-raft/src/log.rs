use slog::Drain;
use slog::Logger;
use crate::config::RaftConfig;

pub fn get_root_logger(config: RaftConfig) -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    Logger::root(drain, o!("id" => config.id))
}
