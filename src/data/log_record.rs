/// LogRecordPos description of a record position with file id and offset
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// Append log format to a file
/// its behavior is similar to a LSM log file
pub struct LogRecord {
    key: Vec<u8>,
}
