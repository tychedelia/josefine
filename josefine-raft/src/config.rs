use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr};
use std::net::ToSocketAddrs;
use std::time::Duration;

use crate::raft::NodeId;
use crate::config;
use std::net::SocketAddr;

#[serde(default)]
#[derive(Clone, Debug, Serialize, Deserialize)]
/// The configuration for this Raft instance.
pub struct RaftConfig {
    /// The id used for this instance. Should be unique.
    pub id: NodeId,
    /// The ip address to listen for requests on in TCP implmentations.
    pub ip: IpAddr,
    /// The port to listen for request on in TCP implementations.
    pub port: u16,
    /// A list of addresses to query for cluster membership.
    pub nodes: Vec<String>,
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
    /// Validates the configuration, ensuring all values make sense.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.protocol_version > MAX_PROTOCOL_VERSION {
            return Err(ConfigError::new("Invalid protocol version."));
        }
        if self.id == 0 {
            return Err(ConfigError::new("Id cannot be zero."));
        }
        if self.port < 1023 {
            return Err(ConfigError::new("Port value too low."));
        }
        if self.heartbeat_timeout < Duration::from_millis(5) {
            return Err(ConfigError::new("Heartbeat timeout is too low."));
        }
        if self.election_timeout < Duration::from_millis(5) {
            return Err(ConfigError::new("Election timeout is too low."));
        }
        if self.commit_timeout < Duration::from_millis(1) {
            return Err(ConfigError::new("Commit timeout is too low."));
        }
        if self.snapshot_interval < Duration::from_millis(5) {
            return Err(ConfigError::new("Snapshot interval is too low."));
        }

        Ok(())
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        let ip = resolve("localhost")
            .unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));

        let id = match ip {
            IpAddr::V4(ipv4) => {
                ipv4.into()
            }
            IpAddr::V6(ipv6) => {
                ipv6.to_ipv4().unwrap().into()
            }
        };

        RaftConfig {
            id,
            ip,
            port: 6669,
            nodes: vec![],
            protocol_version: 0,
            heartbeat_timeout: Duration::from_millis(1000),
            election_timeout: Duration::from_millis(1000),
            commit_timeout: Duration::from_millis(50),
            max_append_entries: 64,
            snapshot_interval: Duration::from_secs(120),
            snapshot_threshold: 8192,
        }
    }
}

#[derive(Debug)]
/// Represents an error with the configuration.
pub struct ConfigError {
    reason: String
}

impl ConfigError {
    /// Create an error with the provided reason.
    pub fn new(reason: &str) -> ConfigError {
        ConfigError { reason: reason.to_string() }
    }
}

impl Error for ConfigError {
    fn description(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

fn resolve(host: &str) -> Option<IpAddr> {
    (host, 0).to_socket_addrs()
        .map(|iter| iter
            .map(|socket_address| socket_address.ip())
            .nth(0)).unwrap()
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
        };

        let res = config.validate();
        assert_eq!(true, res.is_err());
    }
}
