use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};

use crate::util::timestamp;

// Metadata part
#[derive(Debug)]
pub struct Meta {
    pub(crate) crc: u32,
    pub(crate) timestamp: i64,
    pub(crate) expires: u32,
    pub(crate) key_size: usize,
    pub(crate) data_size: usize,
}

impl From<Meta> for Bytes {
    fn from(value: Meta) -> Self {
        let mut buffer = BytesMut::new();
        let crc = value.crc.to_be_bytes();
        let timestamp = value.timestamp.to_be_bytes();
        let expires = value.expires.to_be_bytes();
        let key_size = value.key_size.to_be_bytes();
        let data_size = value.data_size.to_be_bytes();
        buffer.put_slice(&crc);
        buffer.put_slice(&timestamp);
        buffer.put_slice(&expires);
        buffer.put_slice(&key_size);
        buffer.put_slice(&data_size);
        buffer.freeze()
    }
}

impl TryFrom<Vec<u8>> for Meta {
    type Error = crate::BarrilError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        todo!()
    }
}

// Data part
#[derive(Debug)]
pub struct Entry {
    pub(crate) meta: Meta,
    pub(crate) key: String, // we might Cow here
    pub(crate) data: Bytes,
}

impl From<Entry> for Bytes {
    fn from(value: Entry) -> Self {
        let mut buffer = BytesMut::new();
        let meta: Bytes = value.meta.into();
        let key = value.key.as_bytes();
        buffer.put(meta);
        buffer.put(key);
        buffer.put(value.data);
        buffer.freeze()
    }
}

const CKSUM: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);

impl Entry {
    pub fn new(key: String, data: Bytes, expires: u32) -> Entry {
        let meta = Meta {
            data_size: data.len(),
            key_size: key.len(),
            expires: expires,
           timestamp:  timestamp(),
           crc: CKSUM.checksum(&data)
        };
        Entry {
            meta,
            data,
            key
        }
    }
}