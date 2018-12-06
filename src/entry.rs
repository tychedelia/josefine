use std::io::Cursor;
use std::convert::From;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, PartialEq)]
pub struct Entry {
    offset: u32,
    position: u32,
}

impl Entry {
    pub fn new(offset: u32, position: u32) -> Entry {
        Entry {
            offset,
            position,
        }
    }
}

impl From<Entry> for Vec<u8> {
    fn from(entry: Entry) -> Self {
        let mut bytes = vec![];
        bytes.write_u32::<BigEndian>(entry.offset).unwrap();
        bytes.write_u32::<BigEndian>(entry.position).unwrap();
        bytes
    }
}

impl From<&[u8]> for Entry {
    fn from(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), 8);
        let offset_vec: Vec<u8> = bytes.iter().take(4).cloned().collect();
        let offset = Cursor::new(offset_vec).read_u32::<BigEndian>().unwrap();
        let position_vec: Vec<u8> = bytes.iter().skip(4).take(4).cloned().collect();
        let position = Cursor::new(position_vec).read_u32::<BigEndian>().unwrap();
        Entry {
            offset,
            position,
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn entry_to_bytes() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        let res_bytes: Vec<u8> = entry.into();
        assert_eq!(bytes, res_bytes);
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        let res_bytes: Vec<u8> = entry.into();
        assert_eq!(bytes, res_bytes);
    }

    #[test]
    fn byes_to_entry() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        assert_eq!(entry, super::Entry::from(bytes.as_ref()));
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        assert_eq!(entry, super::Entry::from(bytes.as_ref()));
    }
}