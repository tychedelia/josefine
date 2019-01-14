struct Progress<T> {
    node_id: u64,
    state: T
}

struct Probe {}

impl Progress<Probe> {

}

impl From<Progress<Replicate>> for Progress<Probe> {
    fn from(progress: Progress<Replicate>) -> Self {
        Progress { node_id: 0, state: Probe {} }
    }
}

struct Snapshot {}

impl Progress<Snapshot> {

}

struct Replicate {}

impl Progress<Replicate> {
}
