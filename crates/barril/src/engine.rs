use crate::{record::KeyMap, DataFile};

pub struct Engine {
    active_data: DataFile,
    key_map: KeyMap,
}
