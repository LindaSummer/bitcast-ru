use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::fio;

use crate::error::Result;

use super::log_record::LogRecord;

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
    pub fn new(file_dir: PathBuf, fid: u32) -> Result<Self> {
        todo!()
    }

    pub fn write_offset(&self) -> u64 {
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

    pub fn read(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }
}
