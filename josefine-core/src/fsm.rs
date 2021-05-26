use josefine_raft::fsm::Fsm;

#[derive(Debug)]
pub struct JosefineFsm;

impl Fsm for JosefineFsm{
    fn transition(&mut self, input: Vec<u8>) -> josefine_raft::error::Result<Vec<u8>> {
        todo!()
    }
}