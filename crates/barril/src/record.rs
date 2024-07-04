
// Metadata part
#[derive(Debug)]
pub struct Meta {
    crc: u32,
    timestamp: u32,
    expires: u32,
    key_size: u32,
    data_size: u32
}

impl From<Meta> for Vec<u8> {

    fn from(value: Meta) -> Self {
        let crc = value.crc.to_be_bytes();
        let timestamp = value.timestamp.to_be_bytes();
        let expires = value.expires.to_be_bytes();
        let key_size = value.key_size.to_be_bytes();
        let data_size = value.data_size.to_be_bytes();
        [crc,timestamp,expires,key_size,data_size].concat()
    }
}

impl TryFrom<Vec<u8>> for Meta {
    type Error = crate::BarrilError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() != 20 { // no header will be shorter than 20 bytes
            return Err(crate::BarrilError::MalformedData)
        };
        todo!()
    }
}

// Data part
#[derive(Debug)]
pub struct Entry {
    meta: Meta,
    key: String,
    data: Vec<u8>
}

impl From<Entry> for Vec<u8> {
    fn from(value: Entry) -> Self {
        let meta: Vec<u8> = value.meta.into();
        let key = value.key.as_bytes();
        let data = value.data.as_slice();
        [meta, key.to_vec(), data.to_vec()].concat()
    }
}

impl TryFrom<Vec<u8>> for Entry {
    type Error = crate::BarrilError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() < 20 { // we require at least a header
            return Err(crate::BarrilError::MalformedData)
        };
        todo!()
    }
}