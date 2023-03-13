use std::{borrow::Borrow, collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Errors, Result},
    index,
    options::Options,
};

pub struct Engine {
    options: Options,

    active_file: Arc<RwLock<DataFile>>, // current active file
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old files
    indexer: Box<dyn index::Indexer>,   // memory index manager
}

impl Engine {
    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORAML,
        };

        let record_pos = self.append_log_record(record.borrow())?;

        match self.indexer.put(key.to_vec(), record_pos) {
            true => Ok(()),
            false => Err(Errors::FailToUpdateIndex),
        }
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record_pos = match self.indexer.get(key.to_vec()) {
            Some(record) => Ok(record),
            None => Err(Errors::KeyNotFound),
        }?;

        let active_file = self.active_file.read();
        let old_files = self.old_files.read();
        let hint_file = match active_file.file_id() == record_pos.file_id {
            true => {
                drop(old_files);
                active_file.borrow()
            }
            false => {
                drop(active_file);
                let file = old_files
                    .get(record_pos.file_id.borrow())
                    .ok_or_else(|| Errors::DataFileNotFound)?;
                file
            }
        };

        let record = hint_file.read(record_pos.offset)?;
        if record.record_type == LogRecordType::DELETED {
            Err(Errors::KeyNotFound)
        } else {
            Ok(record.value.into())
        }
    }

    // pub fn get(&self, key: &Bytes) -> Result<Bytes> {}

    /// this function is for new log append to a active file.
    /// if current active file is reached threshold, then create a new one and put current file
    /// into old file map
    ///
    /// # Returns
    /// returns a new log's postion record
    ///
    /// # Errors
    ///
    /// This function will return an error if active file sync, create or write failure.
    fn append_log_record(&mut self, record: &LogRecord) -> Result<LogRecordPos> {
        let encode_log = record.encode();
        let mut active_file = self.active_file.write();
        if active_file.write_offset() + encode_log.len() as u64 > self.options.datafile_size {
            active_file.sync()?;
            // let prev_active_file =
            //     DataFile::new(self.options.dir_path.clone(), active_file.file_id())?;
            let mut old_files = self.old_files.write();
            let mut tmp_active_file =
                DataFile::new(self.options.dir_path.clone(), active_file.file_id() + 1)?;
            std::mem::swap(&mut *active_file, &mut tmp_active_file);
            old_files.insert(tmp_active_file.file_id(), tmp_active_file);
        }
        let offset = active_file.write_offset();
        active_file.write(&encode_log)?;
        Ok(LogRecordPos {
            file_id: active_file.file_id(),
            offset,
        })
    }
}
