use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    /// database path of directory
    pub dir_path: PathBuf,
    /// active datafile size threshold
    pub datafile_size: u64,

    /// always sync file when writing
    pub sync_in_write: bool,

    pub index_type: IndexType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: PathBuf::from("/tmp/bitcask-rs-engine"),
            datafile_size: 256 * 1024 * 1024, // 256MB
            sync_in_write: false,
            index_type: IndexType::BtreeMap,
        }
    }
}

#[derive(Clone)]
pub enum IndexType {
    // BtreeMap
    BtreeMap,
    // SkipList
    SkipList,
}

pub struct IndexIteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IndexIteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}
