use std::net::{IpAddr, ToSocketAddrs};
use std::path::PathBuf;
use derive_more::Display;

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Display)]
pub struct BrokerId(pub i32);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broker {
    id: BrokerId,
    ip: IpAddr,
    port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    pub id: BrokerId,
    pub ip: IpAddr,
    pub port: u16,
    pub file: PathBuf,
    pub peers: Vec<Broker>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            id: BrokerId(1),
            ip: resolve("localhost").unwrap(), //unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
            port: 8844,
            file: tempfile::tempdir().unwrap().into_path(),
            peers: vec![]
        }
    }
}

fn resolve(host: &str) -> Option<IpAddr> {
    (host, 0)
        .to_socket_addrs()
        .map(|iter| iter.map(|socket_address| socket_address.ip()).next())
        .unwrap()
}
