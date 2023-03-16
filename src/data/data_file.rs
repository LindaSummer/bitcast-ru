use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use log::error;
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len, DecodeError};

use crate::data::log_record::{LogRecord, LogRecordType, LOG_CRC_SIZE, LOG_TYPE_FLAG_SIZE};
use crate::fio::io_manager::new_io_manager;
use crate::fio::{self};

use crate::error::{Errors, Result};

use super::log_record::{log_record_max_size, ReadLogRecord};

pub const DATAFILE_SEPARATOR: &str = ".";

pub const DATAFILE_NAME_SUFFIX: &str = ".bcdata";

/// datafile for each bitcast file
pub(crate) struct DataFile {
    /// current file id
    file_id: Arc<RwLock<u32>>,
    /// current write cursor offset
    write_offset: Arc<RwLock<u64>>,
    /// io manager for file manuplation
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(file_dir: &PathBuf, fid: u32) -> Result<Self> {
        let io_manager = new_io_manager(PathBuf::from(generate_datafile_name(&file_dir, fid)))?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(fid)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager: io_manager,
        })
    }

    pub fn get_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    pub fn file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn sync(&mut self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn write(&mut self, record: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        let mut header_buf = BytesMut::zeroed(log_record_max_size());
        self.io_manager.read(&mut header_buf, offset)?;

        let record_type = header_buf.get_u8();

        let map_err = |e: DecodeError| {
            error!("failed to decode key size from log record header: {:?}", e);
            Errors::DatabaseFileCorrupted
        };
        let key_size = decode_length_delimiter(&mut header_buf).map_err(map_err)?;
        let value_size = decode_length_delimiter(&mut header_buf).map_err(map_err)?;

        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadEOF);
        }

        let actual_header_size =
            LOG_TYPE_FLAG_SIZE + length_delimiter_len(key_size) + length_delimiter_len(value_size);

        let mut kv_buffer = BytesMut::zeroed(key_size + value_size + LOG_CRC_SIZE);
        self.io_manager
            .read(&mut kv_buffer, offset + actual_header_size as u64)?;

        Ok(ReadLogRecord {
            record: LogRecord {
                key: kv_buffer.get(..key_size).unwrap().to_vec(),
                value: kv_buffer.get(key_size..kv_buffer.len()).unwrap().to_vec(),
                record_type: LogRecordType::from_u8(record_type),
            },
            size: (actual_header_size + key_size + value_size) as u64,
        })
    }

    pub(crate) fn set_offset(&mut self, offset: u64) {
        *self.write_offset.write() = offset
    }
}

fn generate_datafile_name(path: &PathBuf, fid: u32) -> String {
    let file_name = std::format!("{:09}{}", fid, DATAFILE_NAME_SUFFIX);
    String::from(path.join(file_name).to_str().unwrap())
}
