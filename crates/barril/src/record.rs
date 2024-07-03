use std::io::Write;
use bytes::{BufMut, Bytes, BytesMut};
use hashbrown::HashMap;
use crate::util::timestamp;

// Metadata part
#[derive(Debug)]
pub struct Meta {
    pub (crate) crc: u32,
    pub (crate) timestamp: i64,
    pub (crate) expires: u32,
    pub (crate) key_size: usize,
    pub (crate) data_size: usize
}

impl From<Meta> for Bytes {

    fn from(value: Meta) -> Self {
        let mut buffer = BytesMut::zeroed(20);
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
        if value.len() != 20 { // no header will be shorter than 20 bytes
            return Err(crate::BarrilError::DataError)
        };
        todo!()
    }
}

// Data part
#[derive(Debug)]
pub struct Entry {
    pub (crate) meta: Meta,
    pub (crate) key: String, // we might Cow here
    pub (crate) data: Bytes
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

#[derive(Debug)]
pub struct Key{
    timestamp: u32,
    size: u32,
    position: u32,
    id: u32,
}

impl From<Key> for Vec<u8> {

    fn from(value: Key) -> Self {
        let timestamp = value.timestamp.to_be_bytes();
        let size = value.size.to_be_bytes();
        let position = value.position.to_be_bytes();
        let id = value.id.to_be_bytes();
        [timestamp,size,position,id].concat()
    }
}

impl From<&Key> for Vec<u8> {

    // Allocation?
    fn from(value: &Key) -> Self {
        let timestamp = value.timestamp.to_be_bytes();
        let size = value.size.to_be_bytes();
        let position = value.position.to_be_bytes();
        let id = value.id.to_be_bytes();
        [timestamp,size,position,id].concat()
    }
}

impl TryFrom<Vec<u8>> for Key {
    type Error = crate::BarrilError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() != 20 { // no header will be shorter than 20 bytes
            return Err(crate::BarrilError::DataError)
        };
        todo!()
    }
}

#[derive(Debug)]
pub struct KeyMap(HashMap<String,Key>);

impl KeyMap {

    // Keymap format:
    // |- key len |- key |- metadata len |- metadata
    pub (crate) fn serialize(&self) -> Vec<u8> {
        let mut buff = BytesMut::new();
        // TODO: Pulled this number out of my a**
        buff.reserve(self.0.len() * 200);
        let mut writer = buff.writer();
        for (k,v) in self.0.iter() {
            // BytesMut is Infallible
            let serialized_key: Vec<u8> = v.into();
            writer.write(&k.len().to_be_bytes()).unwrap();
            writer.write(k.as_bytes()).unwrap();
            writer.write(&serialized_key.len().to_be_bytes()).unwrap();
            writer.write(&serialized_key).unwrap();
        }
        writer.into_inner().to_vec()
    }
    
}

impl From<Vec<u8>> for KeyMap {
    fn from(value: Vec<u8>) -> Self {
        todo!()
    }
}