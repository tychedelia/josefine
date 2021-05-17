extern crate josefine_raft;

use futures_util::core_reexport::time::Duration;
use josefine_raft::config::RaftConfig;
use josefine_raft::raft::{Node, RaftHandle};
use josefine_raft::JosefineRaft;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread::JoinHandle;

fn new_cluster(ids: Vec<u32>) -> Vec<JosefineRaft> {
    ids.iter()
        .map(|id| {
            let default = RaftConfig::default();
            let config = RaftConfig {
                id: *id,
                port: default.port + *id as u16,
                nodes: ids
                    .iter()
                    .filter(|i| id != *i)
                    .map(|id| Node {
                        id: *id,
                        addr: SocketAddr::new(default.ip, default.port + *id as u16),
                    })
                    .collect(),
                ..default
            };
            JosefineRaft::new(config)
        })
        .collect()
}

struct IntegrationFsm {
    state: u8
}

impl IntegrationFsm {
    fn new() -> Self {
        Self { state: 0 }
    }
}

impl josefine_raft::fsm::Fsm for IntegrationFsm {
    fn transition(&mut self, input: Vec<u8>) -> josefine_raft::error::Result<Vec<u8>> {
        Ok(input)
    }
}

#[test]
fn it_elects() {
    let cluster = new_cluster(vec![1, 2, 3]);

    let join_handles: Vec<JoinHandle<josefine_raft::error::Result<RaftHandle>>> = cluster
        .into_iter()
        .map(|node| {
            std::thread::spawn(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(node.run_for(Duration::from_secs(2), IntegrationFsm::new()))
            })
        })
        .collect();

    let nodes: Vec<RaftHandle> = join_handles
        .into_iter()
        .map(|join| join.join().expect("couldn't join").expect("was not err"))
        .collect();

    let counts = nodes.into_iter().fold(HashMap::new(), |mut xs, x| {
        match x {
            RaftHandle::Follower(_n) => *xs.entry("follower").or_insert(0) += 1,
            RaftHandle::Candidate(_n) => *xs.entry("candidate").or_insert(0) += 1,
            RaftHandle::Leader(_n) => *xs.entry("leader").or_insert(0) += 1,
        }
        xs
    });

    assert_eq!(2, *counts.get("follower").unwrap());
    assert_eq!(1, *counts.get("leader").unwrap());
}
