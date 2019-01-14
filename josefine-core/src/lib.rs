#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod entry;
mod log;
mod index;
mod segment;
mod server;
mod partition;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
