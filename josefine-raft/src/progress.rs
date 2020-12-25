use std::collections::HashMap;

use crate::raft::{LogIndex, NodeId, Node};


#[derive(Debug)]
pub struct ReplicationProgress {
    progress: HashMap<NodeId, NodeProgress>,
}

impl ReplicationProgress {
    pub fn new(nodes: &Vec<Node>) -> ReplicationProgress {
        let mut progress = HashMap::new();
        for node in nodes {
            progress.insert(node.id, NodeProgress::Probe(Progress::new(node.id)));
        }
        ReplicationProgress { progress }
    }

    pub fn get(&self, node_id: NodeId) -> Option<&NodeProgress> {
        self.progress.get(&node_id)
    }

    pub fn get_mut(&mut self, node_id: NodeId) -> Option<&mut NodeProgress> {
        self.progress.get_mut(&node_id)
    }

    pub fn insert(&mut self, node_id: NodeId) {
        self.progress
            .insert(node_id, NodeProgress::Probe(Progress::new(node_id)));
    }

    pub fn committed_index(&self) -> LogIndex {
        let mut indices = Vec::new();
        for progress in self.progress.values() {
            if let NodeProgress::Replicate(progress) = progress {
                indices.push(progress.index);
            }
        }

        indices.sort_by(|a, b| b.cmp(a));
        indices[indices.len() / 2]
    }
}

#[derive(Debug)]
pub enum NodeProgress {
    Probe(Progress<Probe>),
    Replicate(Progress<Replicate>),
    Snapshot(Progress<Snapshot>),
}

impl NodeProgress {
    pub fn new(node_id: NodeId) -> NodeProgress {
        NodeProgress::Probe(Progress::new(node_id))
    }

    pub fn increment(&mut self, idx: LogIndex) -> bool {
        match self {
            NodeProgress::Probe(prog) => prog.increment(idx),
            NodeProgress::Replicate(prog) => prog.increment(idx),
            NodeProgress::Snapshot(prog) => prog.increment(idx),
        }
    }

    pub fn is_active(self) -> bool {
        match self {
            NodeProgress::Probe(prog) => prog.is_active(),
            NodeProgress::Replicate(prog) => prog.is_active(),
            NodeProgress::Snapshot(prog) => prog.is_active(),
        }
    }
}

pub trait ProgressState {
    fn reset(&mut self);
}

pub const MAX_INFLIGHT: u64 = 5;

#[derive(Debug)]
pub struct Progress<T: ProgressState> {
    node_id: NodeId,
    state: T,
    active: bool,
    pub index: LogIndex,
    pub next: LogIndex,
}

impl<T: ProgressState> Progress<T> {
    pub fn reset(&mut self) {
        self.active = false;
        self.state.reset();
    }

    pub fn is_active(self) -> bool {
        self.active
    }

    pub fn increment(&mut self, index: LogIndex) -> bool {
        let updated = if self.index < index {
            self.index = index;
            true
        } else {
            false
        };

        if self.next < index + 1 {
            self.next = index + 1;
        }

        updated
    }
}

#[derive(Debug)]
pub struct Probe {
    paused: bool,
}

impl ProgressState for Probe {
    fn reset(&mut self) {
        self.paused = false
    }
}

impl Progress<Probe> {
    fn new(node_id: NodeId) -> Progress<Probe> {
        Progress {
            node_id,
            state: Probe { paused: false },
            active: false,
            index: 0,
            next: 0,
        }
    }
}

impl From<Progress<Replicate>> for Progress<Probe> {
    fn from(progress: Progress<Replicate>) -> Self {
        Progress {
            node_id: progress.node_id,
            state: Probe { paused: false },
            active: false,
            index: progress.index,
            next: progress.next,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Snapshot {
    pending: u64,
}

impl ProgressState for Snapshot {
    fn reset(&mut self) {
        self.pending = 0;
    }
}

impl Progress<Snapshot> {
    fn _snapshot_fail(&mut self) {
        self.state.pending = 0;
    }
}

#[derive(Debug)]
pub struct Replicate {}

impl ProgressState for Replicate {
    fn reset(&mut self) {}
}

impl Progress<Replicate> {}

#[derive(Debug)]
struct PendingReplication {
    index: usize,
    count: usize,
    size: usize,
    pending: Vec<Option<u64>>,
}

impl PendingReplication {
    fn new(size: usize) -> PendingReplication {
        PendingReplication {
            index: 0,
            count: 0,
            size,
            pending: vec![],
        }
    }

    fn _insert(&mut self, id: u64) {
        let mut next = self.index + self.count;
        let size = self.size;

        if next >= size {
            next -= size;
        }

        if next >= self.pending.capacity() {
            self.pending.resize(self.pending.capacity() * 2, None);
        }

        self.pending[next] = Some(id);
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::progress::NodeProgress;

    #[test]
    fn starts_inactive() {
        let progress = NodeProgress::new(0);
        assert!(!progress.is_active());
    }

    #[test]
    fn starts_in_probe() {
        match NodeProgress::new(0) {
            NodeProgress::Probe(_) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn increments_to_higher() {
        let mut progress = NodeProgress::new(0);
        assert!(progress.increment(666));
    }
}
