use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use std::path::{PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    pub ip: IpAddr,
    pub port: u16,
    pub file: PathBuf
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            ip: resolve("localhost").unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
            port: 8844,
            file: tempfile::tempdir().unwrap().into_path()
        }
    }
}

fn resolve(host: &str) -> Option<IpAddr> {
    (host, 0)
        .to_socket_addrs()
        .map(|iter| iter.map(|socket_address| socket_address.ip()).next())
        .unwrap()
}
