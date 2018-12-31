use std::fs;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::segment::Segment;

pub struct Log {
    path: PathBuf,
    segments: Vec<Segment>,
    active_segment: usize,
    rwlock: RwLock<u8>,
}

impl Log {
    pub fn new(path: &Path) -> Log {
        fs::create_dir_all(&path).expect("Couldn't create log dir");
        let segment = Segment::new(path.to_owned(), 0);
        let segments = vec![segment];
        Log {
            path: path.to_owned(),
            segments,
            active_segment: 0,
            rwlock: RwLock::new(255),
        }
    }

    fn newest_offset(&self) -> u64 {
        self.segments[self.active_segment].next_offset
    }
}

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let lock = self.rwlock.write().expect("Couldn't obtain write lock.");

        if self.segments[self.active_segment].full() {
            let segment = Segment::new(self.path.to_owned(), self.newest_offset());
            self.active_segment = self.segments.len();
            self.segments.push(segment);
        }

        self.segments[self.active_segment].write_all(buf)?;
        Result::Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.segments[self.active_segment].flush()
    }
}

impl Read for Log {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let lock = self.rwlock.read().expect("Couldn't obtain read lock.");
        self.segments[self.active_segment].read(buf)
    }
}

#[cfg(test)]
mod test {
    use std::env;
    use std::fs::File;
    use std::io::Read;
    use std::io::Write;

    #[test]
    fn test_write() {
        let mut path = env::temp_dir();
        path.push("test");
        let mut log = super::Log::new(&path);

        log.write(b"one").unwrap();
        log.write(b"two").unwrap();
        log.write(b"three").unwrap();

        path.push("0.log");
        let mut f = File::open(&path).expect("Log file exists.");
        let mut contents = String::new();
        f.read_to_string(&mut contents).expect("Read contents into string.");
        assert_eq!(contents, "onetwothree");
    }
}
