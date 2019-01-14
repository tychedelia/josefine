use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr};
use std::net::ToSocketAddrs;
use std::time::Duration;

use crate::raft::NodeId;

#[derive(Copy, Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    pub ip: IpAddr,
    pub port: u32,
    pub protocol_version: u32,
    pub heartbeat_timeout: Duration,
    pub election_timeout: Duration,
    pub commit_timeout: Duration,
    pub max_append_entries: u64,
    pub snapshot_interval: Duration,
    pub snapshot_threshold: u64,
}

const MAX_PROTOCOL_VERSION: u32 = 0;

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.protocol_version > MAX_PROTOCOL_VERSION {
            return Err(ConfigError::new("Invalid protocol version."));
        }
        if self.id == 0 {
            return Err(ConfigError::new("Id cannot be zero."));
        }
        if self.port < 1023 || self.port > 65535 {
            return Err(ConfigError::new("Invalid range for port."));
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

//        trace!("Configuration validated successfully.");
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        let ip = resolve("localhost")
            .unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));

        let id = match ip {
            IpAddr::V4(ipv4) => {
                ipv4.into()
            }
            IpAddr::V6(ipv6) => {
                ipv6.to_ipv4().unwrap().into()
            }
        };

        Config {
            id,
            ip,
            port: 6669,
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
pub struct ConfigError {
    reason: String
}

impl ConfigError {
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

    use super::Config;

    #[test]
    fn default() {
        Config::default();
    }

    #[test]
    fn validation() {
        let config = Config {
            id: 0,
            ip: IpAddr::from([0, 0, 0, 0]),
            port: 0,
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
