use async_lock::RwLock;
use bytes::Bytes;

use crate::{record::KeyMap, BarrilError, DataFile};

pub struct Engine {
    active_data: DataFile,
    old_data: Vec<DataFile>,
    key_map: KeyMap,

}


impl Engine {
    async fn put(&self, key:String, value: &[u8]) -> Result<(),BarrilError> {
        todo!()
    }
    async fn read(&self, key: String) -> Result<Bytes, BarrilError> {
        todo!()
    }
}
