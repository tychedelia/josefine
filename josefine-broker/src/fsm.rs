use josefine_core::error::Result;
use josefine_raft::fsm::Fsm;

#[derive(Debug)]
pub struct JosefineFsm;

impl Fsm for JosefineFsm{
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }

    fn query(&mut self, data: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Query {
    Topic { name: String }
}

impl Query {
    pub fn to_command(self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec::<Self>(&self)?)
    }

    pub fn from_command(self, buf: &[u8]) -> Result<Query> {
        Ok(serde_json::from_slice(buf)?)
    }
}