use crate::util::timestamp;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use hashbrown::HashMap;
use std::io::{Read, Write};
use std::mem::size_of;

use crate::BarrilError;

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

impl TryFrom<Bytes> for Meta {
    type Error = crate::BarrilError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() != 20 {
            // no header will be shorter than 20 bytes
            return Err(crate::BarrilError::DataError);
        };
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
            timestamp: timestamp(),
            crc: CKSUM.checksum(&data),
        };
        Entry { meta, data, key }
    }
}

#[derive(Debug)]
pub struct KeyMeta {
    timestamp: u32,
    size: u32,
    position: usize,
    id: u32,
}

impl From<&KeyMeta> for Bytes {
    fn from(value: &KeyMeta) -> Self {
        let mut buffer = BytesMut::new();
        let timestamp = value.timestamp.to_be_bytes();
        let size = value.size.to_be_bytes();
        let position = value.position.to_be_bytes();
        let id = value.id.to_be_bytes();
        buffer.put_slice(&timestamp);
        buffer.put_slice(&size);
        buffer.put_slice(&position);
        buffer.put_slice(&id);
        buffer.freeze()
    }
}

impl TryFrom<Bytes> for KeyMeta {
    type Error = crate::BarrilError;

    fn try_from(_: Bytes) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug)]
pub struct KeyMap(HashMap<String, KeyMeta>); //impl DerefMut?

impl From<&KeyMap> for Bytes {
    // Keymap format:
    // |- key len |- key
    fn from(map: &KeyMap) -> Self {
        let buff = BytesMut::new();
        let mut writer = buff.writer();
        writer.write(&map.0.len().to_be_bytes()).unwrap();
        for (k, v) in map.0.iter() {
            // BytesMut is Infallible
            let meta: Bytes = v.into();
            writer.write(&k.len().to_be_bytes()).unwrap();
            writer.write(k.as_bytes()).unwrap();
            writer.write(&meta).unwrap();
        }
        writer.into_inner().freeze()
    }
}

impl TryFrom<Bytes> for KeyMap {
    type Error = BarrilError;
    fn try_from(value: Bytes) -> Result<KeyMap, Self::Error> {
        let mut hash = hashbrown::HashMap::new();
        let mut reader = value.reader();
        let mut map_size: [u8; size_of::<usize>()] = [0; size_of::<usize>()];
        reader
            .read_exact(&mut map_size)
            .map_err(|_| BarrilError::DataError)?;
        for _ in 1..usize::from_be_bytes(map_size) {
            let mut key_size: [u8; size_of::<usize>()] = [0; size_of::<usize>()];
            reader
                .read_exact(&mut key_size)
                .map_err(|_| BarrilError::DataError)?;
            let mut key_buf = vec![0u8; usize::from_be_bytes(key_size)];
            reader
                .read_exact(&mut key_buf)
                .map_err(|_| BarrilError::DataError)?;
            let key_value = String::from_utf8(key_buf).map_err(|_| BarrilError::DataError)?;
            let mut meta_buf = vec![0u8; 24]; // size of KeyMeta
            reader
                .read_exact(&mut meta_buf)
                .map_err(|_| BarrilError::DataError)?;
            let key_meta: KeyMeta = Bytes::from(meta_buf).try_into()?;
            hash.insert(key_value, key_meta);
        }
        Ok(KeyMap(hash))
    }
}
