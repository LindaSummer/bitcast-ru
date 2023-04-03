use crate::{data::log_record::LogRecordPos, options::IndexType};

use super::btree::BTreeIndexer;

/// Indexr an interface for index implementation
/// it must be concurrent safe
pub trait Indexer: Sync + Send {
    /// add a new entry
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// delete an entry
    fn delete(&self, key: Vec<u8>) -> bool;
    /// get an entry's log position
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
}

pub(crate) fn new_indexer(idx_typ: IndexType) -> impl Indexer {
    match idx_typ {
        IndexType::BtreeMap => BTreeIndexer::new(),
        IndexType::SkipList => todo!(),
    }
}

pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);

    fn seek(&mut self, key: &[u8]);

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
