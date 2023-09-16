use crate::broker::config::BrokerConfig;
use crate::raft::config::RaftConfig;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct JosefineConfig {
    pub raft: RaftConfig,
    pub broker: BrokerConfig,
}

pub fn config<P: AsRef<std::path::Path>>(config_path: P) -> JosefineConfig {
    let settings = config::Config::builder();
    let config = settings
        .add_source(config::File::from(config_path.as_ref()))
        .add_source(config::Environment::with_prefix("JOSEFINE"))
        .build().expect("Could not build configuration");

    config.try_deserialize().expect("Could not deserialize configuration")
}
