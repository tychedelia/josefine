use std::io::Error;

pub struct Config {
    pub id: u64
}

impl Config {
    pub fn init(&self) -> Result<(), Error> {
        Ok(())
    }
}
