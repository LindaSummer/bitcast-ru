use prost::length_delimiter_len;

/// LogRecordPos description of a record position with file id and offset
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// types of a record in a log
#[derive(PartialEq)]
pub enum LogRecordType {
    /// a normal record of a log
    NORAML = 1,

    /// tombstone record of a log
    DELETED = 2,
}

impl LogRecordType {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORAML,
            2 => LogRecordType::DELETED,
            _ => unreachable!(),
        }
    }
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

pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

/// | log_type | key_size | value_size | key | value | crc
pub struct LogRecordHeader {
    pub(crate) log_type: LogRecordType,
    pub(crate) key_size: u32,
    pub(crate) value_size: u32,
    pub(crate) crc: u32,
}

pub(crate) const LOG_CRC_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const LOG_TYPE_FLAG_SIZE: usize = std::mem::size_of::<u8>();

pub(crate) fn log_record_max_size() -> usize {
    LOG_TYPE_FLAG_SIZE + length_delimiter_len(std::u32::MAX as usize) * 2 + LOG_CRC_SIZE
}
