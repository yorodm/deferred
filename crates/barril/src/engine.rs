use async_lock::Mutex;
use bytes::Bytes;

use crate::{record::KeyMap, BarrilError, DataFile};

pub struct Engine {
    active_data: Mutex<DataFile>, // duh
    old_data: Vec<DataFile>,
    key_map: KeyMap,
}

impl Engine {
    pub async fn put(&self, key: String, value: &[u8]) -> Result<(), BarrilError> {
        todo!()
    }
    pub async fn read(&self, key: String) -> Result<Bytes, BarrilError> {
        todo!()
    }
}
