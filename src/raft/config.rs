use std::net::ToSocketAddrs;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

use crate::raft::Node;
use crate::raft::NodeId;
use anyhow::Result;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
/// The configuration for this Raft instance.
pub struct RaftConfig {
    ///
    pub data_directory: PathBuf,
    /// Limit run-time for testing
    pub run_for: Option<Duration>,
    /// The id used for this instance. Should be unique.
    pub id: NodeId,
    /// The ip address to listen for requests on in TCP implmentations.
    pub ip: IpAddr,
    /// The port to listen for request on in TCP implementations.
    pub port: u16,
    /// A list of addresses to query for cluster membership.
    pub nodes: Vec<Node>,
    /// The version of the protocol spoken by this instance.
    pub protocol_version: u32,
    /// The default timeout for a heartbeat.
    pub heartbeat_timeout: Duration,
    /// The default timeout for an election.
    pub election_timeout: Duration,
    ///
    pub commit_timeout: Duration,
    /// Maximum number of entries that can be sent in an append message.
    pub max_append_entries: u64,
    ///
    pub snapshot_interval: Duration,
    ///
    pub snapshot_threshold: u64,
}

const MAX_PROTOCOL_VERSION: u32 = 0;

impl RaftConfig {
    pub fn config(config_path: &std::path::Path) -> RaftConfig {
        let settings = config::Config::builder();
        let config = settings
            .add_source(config::File::from(config_path))
            .add_source(config::Environment::with_prefix("crate::raft"))
            .build()
            .expect("Could not build configuration");

        config
            .try_deserialize()
            .expect("Could not create configuration")
    }

    /// Validates the configuration, ensuring all values make sense.
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version > MAX_PROTOCOL_VERSION {
            return Err(anyhow::anyhow!("invalid protocol version"));
        }
        if self.id == 0 {
            return Err(anyhow::anyhow!("id cannot be 0"));
        }
        if self.port < 1023 {
            return Err(anyhow::anyhow!("port value too low"));
        }
        if self.heartbeat_timeout < Duration::from_millis(5) {
            return Err(anyhow::anyhow!("heartbeat timeout is too low"));
        }
        if self.election_timeout < Duration::from_millis(5) {
            return Err(anyhow::anyhow!("election timeout is too low"));
        }
        if self.commit_timeout < Duration::from_millis(1) {
            return Err(anyhow::anyhow!("commit timeout is too low"));
        }
        if self.snapshot_interval < Duration::from_millis(5) {
            return Err(anyhow::anyhow!("snapshot interval is too low"));
        }

        Ok(())
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        let ip = resolve("localhost").unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));

        let id = match ip {
            IpAddr::V4(ipv4) => ipv4.into(),
            IpAddr::V6(ipv6) => ipv6.to_ipv4().unwrap().into(),
        };

        RaftConfig {
            data_directory: tempdir().unwrap().into_path(),
            run_for: None,
            id,
            ip,
            port: 6669,
            nodes: vec![],
            protocol_version: 0,
            heartbeat_timeout: Duration::from_millis(100),
            election_timeout: Duration::from_millis(1000),
            commit_timeout: Duration::from_millis(50),
            max_append_entries: 64,
            snapshot_interval: Duration::from_secs(120),
            snapshot_threshold: 8192,
        }
    }
}

fn resolve(host: &str) -> Option<IpAddr> {
    (host, 0)
        .to_socket_addrs()
        .map(|iter| iter.map(|socket_address| socket_address.ip()).next())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::time::Duration;

    use super::RaftConfig;

    #[test]
    fn default() {
        RaftConfig::default();
    }

    #[test]
    fn validation() {
        let config = RaftConfig {
            run_for: None,
            id: 0,
            ip: IpAddr::from([0, 0, 0, 0]),
            port: 0,
            nodes: vec![],
            protocol_version: 6666,
            heartbeat_timeout: Duration::from_millis(1), // shouldn't validate
            election_timeout: Duration::from_secs(100),
            commit_timeout: Duration::from_secs(100),
            max_append_entries: 0,
            snapshot_interval: Duration::from_secs(100),
            snapshot_threshold: 0,
            ..Default::default()
        };

        let res = config.validate();
        assert_eq!(true, res.is_err());
    }
}
