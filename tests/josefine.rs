use std::collections::HashMap;
use std::net::SocketAddr;

use josefine::broker::config::Peer;
use josefine::broker::BrokerId;
use josefine::config::JosefineConfig;
use josefine::josefine_with_config;
use tokio::time::Duration;

use josefine::raft::Node;
use josefine::util::Shutdown;

#[derive(Debug)]
struct NodeManager {
    nodes: HashMap<u16, JosefineConfig>,
}

impl NodeManager {
    fn new() -> Self {
        NodeManager {
            nodes: Default::default(),
        }
    }

    fn new_node(&mut self, offset: u16) {
        let mut config: JosefineConfig = Default::default();
        config.raft.id += offset as u32;
        config.raft.port += offset;
        config.broker.id = BrokerId(config.broker.id.0 + offset as i32);
        config.broker.port += offset;
        self.nodes.insert(offset, config);
    }

    fn get_addrs(&self) -> Vec<SocketAddr> {
        self.nodes
            .iter()
            .map(|x| SocketAddr::new(x.1.broker.ip, x.1.broker.port))
            .collect()
    }

    pub async fn run(mut self, _shutdown: Shutdown) -> anyhow::Result<()> {
        let brokers: Vec<Peer> = self
            .nodes
            .iter()
            .map(|x| Peer {
                id: x.1.broker.id,
                ip: x.1.broker.ip,
                port: x.1.broker.port,
            })
            .collect();

        let raft_nodes: Vec<Node> = self
            .nodes
            .iter()
            .map(|x| Node {
                id: x.1.raft.id,
                addr: SocketAddr::new(x.1.raft.ip, x.1.raft.port),
            })
            .collect();

        // build out config
        self.nodes.iter_mut().for_each(|x| {
            x.1.broker.peers = brokers
                .iter()
                .filter(|y| x.1.broker.id != y.id)
                .map(Clone::clone)
                .collect();
            x.1.raft.nodes = raft_nodes
                .iter()
                .filter(|y| x.1.raft.id != y.id)
                .map(Clone::clone)
                .collect();
        });

        let mut tasks = Vec::new();
        let shutdown = Shutdown::new();
        for (_, config) in self.nodes.into_iter() {
            let shutdown = shutdown.clone();
            let task = tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(60),
                    josefine_with_config(config, shutdown),
                )
                .await?;
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
    let addrs = nodes.get_addrs();
    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    tokio::spawn(async move { nodes.run(s).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let client = KafkaClient::new(addrs[0]).await?.connect(shutdown).await?;
    let mut header = RequestHeader::default();
    header.request_api_version = 6;
    header.request_api_key = ApiKey::ApiVersionsKey as i16;
    let mut req = ApiVersionsRequest::default();
    req.client_software_name = StrBytes::from_str("test");
    req.client_software_version = StrBytes::from_str("1.0.0");
    client
        .send(header, RequestKind::ApiVersionsRequest(req))
        .await?;
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn create_topic() -> anyhow::Result<()> {
    let mut nodes = NodeManager::new();
    nodes.new_node(4);
    let addrs = nodes.get_addrs();
    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    tokio::spawn(async move { nodes.run(s).await });
    tokio::time::sleep(Duration::from_secs(2)).await;
    let client = KafkaClient::new(addrs[0]).await?.connect(shutdown).await?;
    let mut header = RequestHeader::default();
    header.request_api_version = 7;
    header.request_api_key = ApiKey::CreateTopicsKey as i16;
    let mut req = CreateTopicsRequest::default();
    req.topics.insert(TopicName(StrBytes::from_str("test")), {
        let mut t = CreatableTopic::default();
        t.replication_factor = 2;
        t.num_partitions = 2;
        t
    });

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        client.send(header, RequestKind::CreateTopicsRequest(req)),
    )
    .await
    .expect("did not timeout");
    match res {
        Ok(res) => {
            if let ResponseKind::CreateTopicsResponse(_res) = res {
                // TODO assert response type
            } else {
                panic!("wrong response type")
            }
        }
        Err(err) => {
            tracing::error!(?err, "could not create topic");
            return Err(anyhow::anyhow!(""));
        }
    };
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn multi_node() -> anyhow::Result<()> {
    let mut nodes = NodeManager::new();
    nodes.new_node(1);
    nodes.new_node(2);
    nodes.new_node(3);
    let addrs = nodes.get_addrs();
    let shutdown = Shutdown::new();
    let s = shutdown.clone();
    tokio::spawn(async move { nodes.run(s).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let client = KafkaClient::new(addrs[0]).await?.connect(shutdown).await?;
    let mut header = RequestHeader::default();
    header.request_api_version = 6;
    header.request_api_key = ApiKey::ApiVersionsKey as i16;
    let mut req = ApiVersionsRequest::default();
    req.client_software_name = StrBytes::from_str("test");
    req.client_software_version = StrBytes::from_str("1.0.0");
    client
        .send(header, RequestKind::ApiVersionsRequest(req))
        .await?;
    Ok(())
}
