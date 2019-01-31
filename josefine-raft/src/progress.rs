use crate::raft::NodeId;
use std::collections::HashMap;
use crate::raft::NodeMap;

pub struct ReplicationProgress {
    progress: HashMap<NodeId, ProgressHandle>,
    nodes: NodeMap,
}

impl ReplicationProgress {
    pub fn new(nodes: NodeMap) -> ReplicationProgress {
        let mut progress = HashMap::new();
        for (id, _) in nodes.read().unwrap().iter() {
            progress.insert(*id, ProgressHandle::new(*id));
        }

        ReplicationProgress {
            progress,
            nodes,
        }
    }

    pub fn get(&self, node_id: NodeId) -> Option<&ProgressHandle> {
        return self.progress.get(&node_id);
    }

    pub fn get_mut(&mut self, node_id: NodeId) -> Option<&mut ProgressHandle> {
        return self.progress.get_mut(&node_id);
    }

    pub fn insert(&mut self, node_id: NodeId) {
        self.progress.insert(node_id, ProgressHandle::Probe(Progress::new(node_id)));
    }

    pub fn committed_index(&self) -> u64 {
        let mut indices = Vec::new();
        for (_, progress) in &self.progress {
            if let ProgressHandle::Replicate(progress) = progress {
                indices.push(progress.index);
            }
        }

        indices.sort_by(|a, b| b.cmp(a));
        indices[indices.len() / 2]
    }
}

pub enum ProgressHandle {
    Probe(Progress<Probe>),
    Replicate(Progress<Replicate>),
    Snapshot(Progress<Snapshot>),
}

impl ProgressHandle {
    pub fn new(node_id: NodeId) -> ProgressHandle {
        ProgressHandle::Probe(Progress::new(node_id))
    }
}

pub trait ProgressState {
    fn reset(&mut self);
}

pub struct Progress<T: ProgressState> {
    node_id: NodeId,
    state: T,
    active: bool,
    index: u64,
    next: u64,
}

impl<T: ProgressState> Progress<T> {
    pub fn reset(&mut self) {
        self.active = false;
        self.state.reset();
    }

    pub fn increment(&mut self, index: u64) -> bool {
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
            state: Probe {
                paused: false
            },
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
            state: Probe {
                paused: false
            },
            active: false,
            index: progress.index,
            next: progress.next,
        }
    }
}

#[allow(dead_code)]
pub struct Snapshot {
    pending: u64,
}

impl ProgressState for Snapshot {
    fn reset(&mut self) {
        self.pending = 0;
    }
}

impl Progress<Snapshot> {
    fn snapshot_fail(&mut self) {
        self.state.pending = 0;
    }
}

pub struct Replicate {}

impl ProgressState for Replicate {
    fn reset(&mut self) {
    }
}

impl Progress<Replicate> {}

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

    fn insert(&mut self, id: u64) {
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
