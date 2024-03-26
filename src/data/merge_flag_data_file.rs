use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::fio::io_manager::new_io_manager;

use super::{
    data_file::DataFile,
    log_record::{LogRecord, LogRecordPos, LogRecordType},
};
use crate::error::Result;

pub(crate) const HINT_FILE_NAME: &'static str = ".hint";

pub(crate) const MERGE_FLAG_FILE_NAME: &'static str = "_merge.flag";

impl DataFile {
    pub(crate) fn new_hint_file(dir_path: &PathBuf) -> Result<Self> {
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager: new_io_manager(dir_path.join(HINT_FILE_NAME))?,
        })
    }

    pub(crate) fn new_merge_fin_file(dir_path: &PathBuf) -> Result<Self> {
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager: new_io_manager(dir_path.join(MERGE_FLAG_FILE_NAME))?,
        })
    }

    pub(crate) fn write_hint_record(&mut self, key: &Vec<u8>, pos: &LogRecordPos) -> Result<usize> {
        let hint_record = LogRecord {
            key: key.clone(),
            value: pos.encode(),
            record_type: LogRecordType::Normal,
        };
        self.write(&hint_record.encode())
    }
}
