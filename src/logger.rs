use slog::Drain;
use slog::Logger;

lazy_static! {
    pub static ref LOGGER: Logger = {
        println!("{}", std::env::var("RUST_LOG").unwrap());

        let drain = slog_async::Async::default(slog_envlogger::new(
            slog_term::FullFormat::new(slog_term::TermDecorator::new().stderr().build())
                .build()
                .fuse(),
        ));

        let logger = Logger::root(drain.fuse(), o!());
        slog_stdlog::init().unwrap();
        logger
    };
}

pub fn get_root_logger() -> &'static LOGGER {
    &LOGGER
}
