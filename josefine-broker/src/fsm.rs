use josefine_core::error::Result;
use josefine_raft::fsm::Fsm;

#[derive(Debug)]
pub struct JosefineFsm;

impl Fsm for JosefineFsm{
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }
}
