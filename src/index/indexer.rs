use crate::data::log_record::LogRecordPos;

/// Indexr an interface for index implementation
pub trait Indexer: Sync + Send {
    /// add a new entry
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// delete an entry
    fn delete(&self, key: Vec<u8>) -> bool;
    /// get an entry's log position
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
}
