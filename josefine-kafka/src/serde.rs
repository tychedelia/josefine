use serde::{Deserialize, de::Visitor};
use bytes::Buf;
use byteorder::ReadBytesExt;

use crate::error::{Error, Result};

pub struct Deserializer<'de> {
    inner: Reader<'de>,
}

impl <'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Self { inner: Reader::new(input) }
    }
}

struct Reader<'de> {
    pub reader: bytes::buf::Reader<&'de [u8]>,
}

impl <'de> Reader<'de> {
    pub fn new(input: &'de [u8]) -> Self {
        Self {
            reader: input.reader(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.reader.get_ref().is_empty()
    }

    pub fn read_boolean(&mut self) -> bool {
        todo!()
    }

    pub fn read_int8(&mut self) -> i8 {
        todo!()
    }

    pub fn read_int16(&mut self) -> i16 {
        todo!()
    }

    pub fn read_int32(&mut self) -> i32 {
        todo!()
    }

    pub fn read_int64(&mut self) -> i64 {
        todo!()
    }

    pub fn read_uint32(&mut self) -> u32 {
        todo!()
    }

    pub fn read_compact_string(&mut self) -> &'de str {
        todo!()
    }
}


pub fn from_bytes<'a, T>(b: &'a [u8]) -> Result<T>
where T: Deserialize<'a> {
    let mut deserializer = Deserializer::from_bytes(b);
    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

impl<'de, 'a> serde::de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de> {
        let v: u64 = self.inner.reader.read_u64().unwrap();
        if v != 18 {
            panic!("fuuu");
        }

        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de> {
        todo!()
    }

    fn is_human_readable(&self) -> bool {
        false
    }

}

#[cfg(test)]
mod tests {
    use crate::protocol::MessageKind;
    use super::*;

    #[test]
    fn api_request() {
        let bytes: [u8; 53] = [
            0x00, 0x00, 0x00, 0x31, 0x00, 0x12, 0x00, 0x03, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x0d, 0x61, 0x64, 0x6d, 0x69, 0x6e, 0x63,
            0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2d, 0x31, 0x00, 0x12, 0x61,
            0x70, 0x61, 0x63, 0x68, 0x65, 0x2d, 0x6b, 0x61, 0x66, 0x6b,
            0x61, 0x2d, 0x6a, 0x61, 0x76, 0x61, 0x06, 0x32, 0x2e, 0x38,
            0x2e, 0x30, 0x00
        ];

        let msg: MessageKind = from_bytes(&bytes).expect("couldn't deserialize");
    }
}

