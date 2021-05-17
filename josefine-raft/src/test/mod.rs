use crate::fsm::Fsm;

pub(crate) struct TestFsm { state: u8 }

impl Fsm for TestFsm {
    fn transition(&mut self, input: Vec<u8>) -> crate::error::Result<Vec<u8>> {
        let data = input.first().unwrap();
        self.state = *data;
        Ok(input)
    }
}