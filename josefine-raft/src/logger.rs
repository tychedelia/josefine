use slog::Drain;
use slog::Logger;

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
