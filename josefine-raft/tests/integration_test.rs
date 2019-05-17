extern crate josefine_raft;

use josefine_raft::config::RaftConfig;
use josefine_raft::raft::{Node, RaftHandle};
use std::time::Duration;
use std::thread;

#[test]
fn it_runs() {
//    let config = RaftConfig {
//        run_for: Some(Duration::from_secs(1)),
//        port: 5440,
//        nodes: vec![Node {
//            id: 0,
//            addr: "127.0.0.1:5440".parse().unwrap(),
//        }],
//        ..RaftConfig::default()
//    };
//
//    let josefine = JosefineBuilder::new()
//        .with_config(config)
//        .build();
//    let raft = josefine.wait();
//
//    match raft {
//        RaftHandle::Leader(_) => {},
//        _ => panic!("is not leader!"),
//    }
}

#[test]
fn three_node_cluster() {
//    let nodes: Vec<Josefine> = vec![1, 2, 3].iter()
//        .map(|num| {
//            RaftConfig {
//                id: *num,
//                run_for: Some(Duration::from_secs(3)),
//                port: 5440 + *num as u16,
//                nodes: vec![1, 2, 3].iter()
//                    .map(|num| {
//                        Node {
//                            id: *num as u32,
//                            addr: format!("127.0.0.1:{}", 5440 + *num as u16).parse().unwrap(),
//                        }
//                    })
//                    .collect(),
//                ..RaftConfig::default()
//            }
//        })
//        .map(|config| {
//           JosefineBuilder::new()
//                    .with_config(config)
//                    .build()
//        })
//        .collect();
//
//    for node in nodes {
//        let _raft = node.wait();
//
//    }
}