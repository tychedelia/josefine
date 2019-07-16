extern crate josefine_raft;

use josefine_raft::config::RaftConfig;

use std::time::Duration;
use std::thread;
use josefine_raft::{JosefineBuilder, JosefineRaft};
use josefine_raft::raft::Node;

use actix::{Actor, Arbiter, AsyncContext, Context, Handler, Recipient, Supervised, Supervisor, System, SystemService, Message};
use tokio::prelude::Future;
use crate::josefine_raft::raft::State;
use crate::josefine_raft::raft::RaftActor;
use crate::josefine_raft::raft::RaftHandle;

struct DebugStateMessage;

impl Message for DebugStateMessage {
    type Result = Result<State, std::io::Error>;
}

impl Handler<DebugStateMessage> for RaftActor {
    type Result = Result<State, std::io::Error>;

    fn handle(&mut self, _msg: DebugStateMessage, _ctx: &mut Self::Context) -> Self::Result {
        let raft = self.unwrap();
        let state = match &raft {
            RaftHandle::Follower(raft) => Ok(raft.state.clone()),
            RaftHandle::Candidate(raft) => Ok(raft.state.clone()),
            RaftHandle::Leader(raft) => Ok(raft.state.clone()),
        };

        self.raft = Some(raft);
        state
    }
}


macro_rules! test_node {
        ($timeout:literal, $config:expr, $test:expr) => {
           struct TestActor;

            impl Actor for TestActor {
                type Context = Context<Self>;


                fn started(&mut self, ctx: &mut Self::Context) {
                    ctx.run_later(Duration::from_secs($timeout), |_act, _ctx| {
                        let raft = System::current().registry().get::<RaftActor>();
                        let _state = raft.send(DebugStateMessage)
                            .map(|res| {
                                let state = res.unwrap();
                                $test(state)
                            })
                            .wait();

                        System::current().stop();
                    });
                }
            }


            let josefine = JosefineBuilder::new()
                .with_config($config)
                .with_actor(TestActor)
                .build()
                .run();
        }
    }

#[test]
fn it_elects() {

    let config = RaftConfig {
                id: 1,
                ..Default::default()
            };
    test_node!(5, config, |state: State| {
            assert!(state.voted_for.is_some());
            assert_eq!(state.voted_for.unwrap(), 1);
            assert_eq!(state.current_term, 1);
        });
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
            thread::spawn(move || {
                struct Hi {};
                test_node!(3, config, |state: State| {});
            });

            1
        })
        .collect();

}