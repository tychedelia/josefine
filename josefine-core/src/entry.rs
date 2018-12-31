use std::convert::From;
use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Entry {
    pub offset: u64,
    pub position: u64,
}

impl Entry {
    pub fn new(offset: u64, position: u64) -> Entry {
        Entry {
            offset,
            position,
        }
    }
}

impl From<Entry> for Vec<u8> {
    fn from(entry: Entry) -> Self {
        let mut bytes = vec![];
        bytes.write_u64::<BigEndian>(entry.offset).unwrap();
        bytes.write_u64::<BigEndian>(entry.position).unwrap();
        bytes
    }
}

impl From<&[u8]> for Entry {
    fn from(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), 16);
        let offset_vec: Vec<u8> = bytes.iter().take(8).cloned().collect();
        let offset = Cursor::new(offset_vec).read_u64::<BigEndian>().unwrap();
        let position_vec: Vec<u8> = bytes.iter().skip(8).take(16).cloned().collect();
        let position = Cursor::new(position_vec).read_u64::<BigEndian>().unwrap();
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
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        let res_bytes: Vec<u8> = entry.into();
        assert_eq!(bytes, res_bytes);
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        let res_bytes: Vec<u8> = entry.into();
        assert_eq!(bytes, res_bytes);
    }

    #[test]
    fn byes_to_entry() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        assert_eq!(entry, super::Entry::from(bytes.as_ref()));
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        assert_eq!(entry, super::Entry::from(bytes.as_ref()));
    }

    #[test]
    #[should_panic]
    fn panics_length() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        super::Entry::from(bytes.as_ref());
    }
}
