use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::time::Duration;
use josefine::broker::config::{Broker, BrokerId};
use josefine::config::JosefineConfig;
use josefine::josefine_with_config;
use josefine::raft::config::RaftConfig;
use josefine::raft::Node;

struct NodeManager {
    nodes: HashMap<u16, JosefineConfig>
}

impl NodeManager {
    fn new() -> Self {
        NodeManager {
            nodes: Default::default()
        }
    }

    fn new_node(&mut self, offset: u16) {
        let mut config: JosefineConfig = Default::default();
        config.raft.id = config.raft.id + offset as u32;
        config.raft.port = config.raft.port + offset;
        config.broker.id = BrokerId(config.broker.id.0 + offset as i32);
        config.broker.port = config.broker.port + offset;
        self.nodes.insert(offset, config);
    }

    pub async fn run(mut self, shutdown:(
        tokio::sync::broadcast::Sender<()>,
        tokio::sync::broadcast::Receiver<()>,
    )) -> anyhow::Result<()> {
        let brokers: Vec<Broker> = self.nodes.iter().map(|x| Broker {
            id: x.1.broker.id,
            ip: x.1.broker.ip,
            port: x.1.broker.port,
        })
            .collect();

        let raft_nodes: Vec<Node> = self.nodes.iter()
            .map(|x| Node { id: x.1.raft.id, addr: SocketAddr::new(x.1.raft.ip, x.1.raft.port) })
            .collect();

        // build out config
        self.nodes.iter_mut()
            .for_each(|x| {
                x.1.broker.peers = brokers.iter().filter(|y | x.1.broker.id != y.id).map(Clone::clone).collect();
                x.1.raft.nodes = raft_nodes.iter().filter(|y| x.1.raft.id != y.id).map(Clone::clone).collect();
            });

        let mut tasks = Vec::new();
        for (_, config)  in self.nodes.into_iter() {
            let (tx, rx) = (shutdown.0.clone(), shutdown.0.subscribe());
            let task = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(1), josefine_with_config(config, (tx, rx))).await?;
                Result::<_, anyhow::Error>::Ok(())
            });

            tasks.push(task);
        }

        let tasks = futures::future::join_all(tasks).await;

        for x in tasks {
            let _ = x??;
        }

        Ok(())
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn single_node() -> anyhow::Result<()> {
    let mut nodes = NodeManager::new();
    nodes.new_node(0);
    let shutdown = tokio::sync::broadcast::channel(1);
    let tx = shutdown.0.clone();
    let nodes = tokio::spawn(async move { nodes.run(shutdown).await });
    let shutdown = tokio::spawn(async move { tx.send(()) });
    tokio::try_join!(nodes, shutdown)?;
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn multi_node() -> anyhow::Result<()> {
    let mut nodes = NodeManager::new();
    nodes.new_node(0);
    nodes.new_node(1);
    nodes.new_node(2);
    let shutdown = tokio::sync::broadcast::channel(1);
    let tx = shutdown.0.clone();
    let nodes = tokio::spawn(async move { nodes.run(shutdown).await });
    let shutdown = tokio::spawn(async move { tx.send(()) });
    tokio::try_join!(nodes, shutdown)?;
    Ok(())
}