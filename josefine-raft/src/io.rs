use crate::raft::Entry;
use crate::raft::NodeId;


/// Defines all IO (i.e. persistence) related behavior. Making our implementation generic over
/// IO is slightly annoying, but allows us, e.g., to implement different backend strategies for
/// persistence, which makes it easier for testing and helps isolate the "pure logic" of the state
/// machine from persistence concerns.
pub trait Io {
    /// Append the provided entries to the commit log.
    fn append(&mut self, entries: &mut Vec<Entry>);
    ///
    fn heartbeat(&mut self, id: NodeId);
}

/// Simple IO impl used for mocking + testing.
pub struct MemoryIo {
    /// The log as an in memory list of entries.
    entries: Vec<Entry>
}

impl Default for MemoryIo {
    fn default() -> Self {
        MemoryIo { entries: Vec::new() }
    }
}

impl MemoryIo {
    /// Obtain a new instance of the memory io implementation.
    pub fn new() -> Self {
        MemoryIo { entries: Vec::new() }
    }
}

impl Io for MemoryIo {
    fn append(&mut self, entries: &mut Vec<Entry>) {
        self.entries.append(entries);
    }

    fn heartbeat(&mut self, _id: NodeId) {
        unimplemented!()
    }
}
