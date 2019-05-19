extern crate josefine_raft;

use josefine_raft::config::RaftConfig;

use std::time::Duration;
use std::thread;
use josefine_raft::JosefineBuilder;
use josefine_raft::raft::Node;

#[test]
fn it_runs() {
    thread::spawn(|| {
        let config = RaftConfig {
            run_for: Some(Duration::from_secs(1)),
            port: 5440,
            nodes: vec![],
            ..RaftConfig::default()
        };

        JosefineBuilder::new()
            .with_config(config)
            .build();
    });

    thread::sleep(Duration::from_secs(1));
}

#[test]
fn three_node_cluster() {
    let _res: Vec<u64> = vec![1, 2, 3].iter()
        .map(|num| {
            RaftConfig {
                id: *num,
                run_for: Some(Duration::from_secs(3)),
                port: 5440 + *num as u16,
                nodes: vec![1, 2, 3].iter()
                    .filter(|x| *x != num)
                    .map(|num| {
                        Node {
                            id: *num as u32,
                            addr: format!("127.0.0.1:{}", 5440 + *num as u16).parse().unwrap(),
                        }
                    })
                    .collect(),
                ..RaftConfig::default()
            }
        })
        .map(|config| {
            thread::sleep(Duration::from_secs(1));
            thread::spawn(move || {
                JosefineBuilder::new()
                    .with_config(config)
                    .build()
            });

            1
        })
        .collect();

    thread::sleep(Duration::from_secs(10));
}