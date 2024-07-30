use crate::broker::BrokerId;
use std::net::IpAddr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broker {
    pub id: BrokerId,
    pub ip: IpAddr,
    pub port: u16,
}
