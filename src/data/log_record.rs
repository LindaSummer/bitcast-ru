/// LogRecordPos description of a record position with file id and offset
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// types of a record in a log
#[derive(PartialEq)]
pub enum LogRecordType {
    /// A normal record of a log
    NORAML = 1,

    /// tombstone record of a log
    DELETED = 2,
}

/// Append log format to a file
/// its behavior is similar to a LSM log file
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        todo!()
    }
}
