use std::net::IpAddr;
use crate::broker::BrokerId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broker {
    pub id: BrokerId,
    pub ip: IpAddr,
    pub port: u16,
}