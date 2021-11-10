use kafka_protocol::protocol::StrBytes;
use bytes::Bytes;;

pub trait ToStrBytes {
    fn to_str_bytes(&self) -> StrBytes;
}

impl ToStrBytes for String {
    fn to_str_bytes(&self) -> StrBytes {
        unsafe { StrBytes::from_utf8_unchecked(Bytes::from(self)) }
    }
}