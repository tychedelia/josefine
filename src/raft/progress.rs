use std::{
    collections::{HashMap, VecDeque},
    convert::TryInto,
};

use crate::raft::{LogIndex, NodeId};

#[derive(Debug)]
pub struct ReplicationProgress {
    progress: HashMap<NodeId, NodeProgress>,
}

impl ReplicationProgress {
    pub fn new(nodes: Vec<NodeId>) -> ReplicationProgress {
        assert!(!nodes.is_empty());

        let mut progress = HashMap::new();
        for node_id in nodes {
            progress.insert(node_id, NodeProgress::Probe(Progress::new(node_id)));
        }
        ReplicationProgress { progress }
    }

    pub fn get(&self, node_id: NodeId) -> Option<&NodeProgress> {
        self.progress.get(&node_id)
    }

    pub fn get_mut(&mut self, node_id: NodeId) -> Option<&mut NodeProgress> {
        self.progress.get_mut(&node_id)
    }

    pub fn remove(&mut self, node_id: NodeId) -> Option<NodeProgress> {
        self.progress.remove(&node_id)
    }

    pub fn insert(&mut self, node_id: NodeId) {
        self.progress
            .insert(node_id, NodeProgress::Probe(Progress::new(node_id)));
    }

    pub fn advance(&mut self, node_id: NodeId, index: LogIndex) {
        let node = self.remove(node_id).expect("the node does not exist");
        let node = node.advance(index);
        self.progress.insert(node_id, node);
    }

    pub fn committed_index(&self) -> LogIndex {
        let mut indices = Vec::new();
        for progress in self.progress.values() {
            match progress {
                NodeProgress::Probe(pr) => indices.push(pr.index),
                NodeProgress::Replicate(pr) => indices.push(pr.index),
                _ => panic!(),
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

    /// Advance the progress to the provided index.
    pub fn advance(self, idx: LogIndex) -> Self {
        match self {
            NodeProgress::Probe(mut prog) => {
                if prog.increment(idx) {
                    Self::Replicate(Progress::from(prog))
                } else {
                    Self::Probe(prog)
                }
            }
            NodeProgress::Replicate(mut prog) => {
                if prog.increment(idx) {
                    Self::Replicate(prog)
                } else {
                    Self::Probe(Progress::from(prog))
                }
            }
            _ => panic!(),
        }
    }

    pub fn is_active(&self) -> bool {
        match self {
            NodeProgress::Probe(prog) => prog.is_active(),
            NodeProgress::Replicate(prog) => prog.is_active(),
            NodeProgress::Snapshot(prog) => prog.is_active(),
        }
    }

    pub fn index(&self) -> LogIndex {
        match self {
            NodeProgress::Probe(prog) => prog.index,
            NodeProgress::Replicate(prog) => prog.index,
            NodeProgress::Snapshot(prog) => prog.index,
        }
    }
}

pub trait ProgressState {
    fn reset(&mut self);
}

pub const MAX_INFLIGHT: u64 = 5;

#[derive(Debug)]
pub struct Progress<T: ProgressState> {
    pub node_id: NodeId,
    pub state: T,
    pub active: bool,
    pub index: LogIndex,
    pub next: LogIndex,
}

impl<T: ProgressState> Progress<T> {
    pub fn reset(&mut self) {
        self.active = false;
        self.state.reset();
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
            next: 1,
        }
    }

    fn is_active(&self) -> bool {
        !self.state.paused
    }
}

impl From<Progress<Replicate>> for Progress<Probe> {
    fn from(progress: Progress<Replicate>) -> Self {
        Progress {
            node_id: progress.node_id,
            state: Probe { paused: false },
            active: progress.active,
            index: progress.index,
            next: progress.next,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
pub struct Snapshot {
    /// Current index of the pending snapshot for this progress.
    /// If there is a pending snapshot, replication progress is halted
    /// until the snapshot is complete.
    pub pending: Option<LogIndex>,
}

impl ProgressState for Snapshot {
    fn reset(&mut self) {
        self.pending = None;
    }
}

impl Progress<Snapshot> {
    pub fn is_active(&self) -> bool {
        self.active
    }

    fn _snapshot_fail(&mut self) {
        self.state.pending = None;
    }
}

#[derive(Debug)]
pub struct Replicate {
    pub inflight: VecDeque<LogIndex>,
}

impl ProgressState for Replicate {
    fn reset(&mut self) {}
}

impl Progress<Replicate> {
    /// The replication is active as long as there are empty spots in the inflight buffer.
    pub fn is_active(&self) -> bool {
        self.state.inflight.capacity() > self.state.inflight.len()
    }
}

impl From<Progress<Probe>> for Progress<Replicate> {
    fn from(progress: Progress<Probe>) -> Self {
        Progress {
            node_id: progress.node_id,
            state: Replicate {
                inflight: VecDeque::with_capacity(MAX_INFLIGHT.try_into().unwrap()),
            },
            active: progress.active,
            index: progress.index,
            next: progress.next,
        }
    }
}

#[derive(Debug)]
pub struct PendingReplication {
    /// The earliest index in the pending buffer.
    index: usize,
    /// The number of items in the pending buffer.
    count: usize,
    size: usize,
    pending: Vec<Option<LogIndex>>,
}

#[cfg(test)]
mod tests {
    use crate::raft::progress::{NodeProgress, ReplicationProgress};

    #[test]
    fn starts_active() {
        let progress = NodeProgress::new(0);
        assert!(progress.is_active());
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
        let progress = NodeProgress::new(0);
        let progress = progress.advance(666);
        assert!(progress.is_active());
        assert_eq!(progress.index(), 666);
    }

    #[test]
    #[should_panic]
    fn cannot_construct_empty() {
        let _ = ReplicationProgress::new(vec![]);
    }
}
