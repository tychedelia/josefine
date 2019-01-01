use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::net::IpAddr;
use std::net::ToSocketAddrs;
use std::str::FromStr;
use std::time::Duration;
use crate::raft::NodeId;

use log::{info, trace, warn};

pub struct Config {
    pub id: NodeId,
    pub protocol_version: u64,
    pub heartbeat_timeout: Duration,
    pub election_timeout: Duration,
    pub commit_timeout: Duration,
    pub max_append_entries: u64,
    pub snapshot_interval: Duration,
    pub snapshot_threshold: u64,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.id == 0 {
            return Err(ConfigError::new("Id cannot be zero."));
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
        if self.max_append_entries <= 0 {
            return Err(ConfigError::new("Max append entries must be positive"));
        }
        if self.snapshot_interval < Duration::from_millis(5) {
            return Err(ConfigError::new("Snapshot interval is too low."));
        }

        trace!("Configuration validated successfully.");
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        let ip = resolve("localhost").unwrap();
        let id= match ip {
            IpAddr::V4(ipv4) => {
                ipv4.into()
            },
            IpAddr::V6(ipv6) => {
                ipv6.to_ipv4().unwrap().into()
            },
        };

        Config {
            id,
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
