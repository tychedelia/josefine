use crate::raft::Entry;
use crate::raft::LogIndex;
use crate::raft::Term;
use std::ops::Index;

pub struct Log {
    data: Vec<Entry>
}

impl Default for Log {
    fn default() -> Self {
        Log {
            data: vec![]
        }
    }
}

impl Log {
    pub fn new() -> Log {
        Default::default()
    }

    pub fn get_index(index: &LogIndex) -> usize {
        (index - 1) as usize
    }

    pub fn check_term(&self, index: &LogIndex, term: &Term) -> bool {
        if let Some(entry) = self.get(index) {
            if &entry.term == term {
                true
            } else {
                false
            }
         } else {
            false
        }
    }

    pub fn get(&self, index: &LogIndex) -> Option<&Entry> {
        self.data.get(Self::get_index(index))
    }

    pub fn append(&mut self, entry: Entry) {
        self.data.insert((entry.index - 1) as usize, entry);
    }
}


