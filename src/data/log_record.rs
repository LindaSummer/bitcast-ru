use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

/// LogRecordPos description of a record position with file id and offset
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// types of a record in a log
#[derive(Debug, PartialEq, Clone, Copy)]
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
#[derive(Debug, PartialEq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    /// encode record as below format
    /// | type | key_size | value_size | key | value | crc |
    pub(crate) fn encode(&self) -> Vec<u8> {
        self.encode_and_crc().0
    }

    pub fn get_crc(&self) -> u32 {
        self.encode_and_crc().1
    }

    fn encode_and_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // type
        buf.put_u8(self.record_type as u8);

        // key size
        let _ = encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        // value size
        let _ = encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // key
        buf.extend_from_slice(&self.key);
        // value
        buf.extend_from_slice(&self.value);

        // crc
        let crc = crc32fast::hash(&buf);
        buf.put_u32_le(crc);

        (buf.to_vec(), crc)
    }

    fn encoded_length(&self) -> usize {
        LOG_TYPE_FLAG_SIZE
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + LOG_CRC_SIZE
    }
}

pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

/// | log_type | key_size | value_size | key | value | crc
// pub struct LogRecordHeader {
//     pub(crate) log_type: LogRecordType,
//     pub(crate) key_size: u32,
//     pub(crate) value_size: u32,
//     pub(crate) crc: u32,
// }

pub(crate) const LOG_CRC_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const LOG_TYPE_FLAG_SIZE: usize = std::mem::size_of::<u8>();

pub(crate) fn log_record_max_size() -> usize {
    LOG_TYPE_FLAG_SIZE + length_delimiter_len(std::u32::MAX as usize) * 2 + LOG_CRC_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_crc() {
        let rec = LogRecord {
            key: "my-key".as_bytes().to_vec(),
            value: "my_value".as_bytes().to_vec(),
            record_type: LogRecordType::NORAML,
        };
        let (vec, crc) = rec.encode_and_crc();
        assert_eq!(vec.len(), 21);
        assert_eq!(crc, 1579242186);

        let rec = LogRecord {
            key: "my-key-1".as_bytes().to_vec(),
            value: vec![],
            record_type: LogRecordType::NORAML,
        };
        let (vec, crc) = rec.encode_and_crc();
        assert_eq!(vec.len(), 15);
        assert_eq!(crc, 4164702405);

        let rec = LogRecord {
            key: "my-key-1".as_bytes().to_vec(),
            value: vec![],
            record_type: LogRecordType::DELETED,
        };
        let (vec, crc) = rec.encode_and_crc();
        assert_eq!(vec.len(), 15);
        assert_eq!(crc, 1641952964);
    }
}
