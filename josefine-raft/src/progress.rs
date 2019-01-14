struct Progress<T> {
    #[allow(dead_code)]
    node_id: u64,
    #[allow(dead_code)]
    state: T,
}

struct Probe {}

impl Progress<Probe> {}

impl From<Progress<Replicate>> for Progress<Probe> {
    fn from(progress: Progress<Replicate>) -> Self {
        Progress { node_id: progress.node_id, state: Probe {} }
    }
}

#[allow(dead_code)]
struct Snapshot {}

impl Progress<Snapshot> {}

struct Replicate {}

impl Progress<Replicate> {}
