use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use log::{info, warn};
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::{DataFile, DATAFILE_NAME_SUFFIX, DATAFILE_SEPARATOR},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Errors, Result},
    index::{self, Indexer},
    options::Options,
};

pub struct Engine {
    options: Options,

    active_file: Arc<RwLock<DataFile>>, // current active file
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old files
    indexer: Box<dyn index::Indexer>,   // memory index manager
}

impl Engine {
    pub fn open(opt: Options) -> Result<Self> {
        check_options(&opt)?;

        let dir_path = opt.dir_path;
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).map_err(|e| {
                warn!("create database directory failed, error: {}", e);
                Errors::FailToCreateDatabaseDirectory
            })?;
        }

        let data_files = load_datafiles(&dir_path.to_path_buf())?;
        let fids = data_files.iter().map(|f| f.file_id());
        let (first_file, old_files) = data_files.split_at(1);
        let old_files = old_files
            .into_iter()
            .map(|f| (f.file_id(), f.clone()))
            .collect::<BTreeMap<_, _>>();

        todo!()
    }

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
                old_files
                    .get(record_pos.file_id.borrow())
                    .ok_or(Errors::DataFileNotFound)?
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

        if self.options.sync_in_write {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.file_id(),
            offset,
        })
    }
}

fn check_options(option: &Options) -> Result<()> {
    option
        .dir_path
        .to_str()
        .ok_or(Errors::InvalidDatabasePath)?;

    if option.datafile_size == 0 {
        return Err(Errors::DatafileSizeTooSmall);
    }

    Ok(())
}

fn load_datafiles(directory_path: &Path) -> Result<Vec<DataFile>> {
    let dir = directory_path.read_dir().map_err(|e| {
        warn!(
            "Error reading directory: {}, error: {}",
            directory_path.to_str().unwrap(),
            e
        );
        Errors::FailToReadDatabaseDirectory
    })?;

    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for entry in dir {
        let entry = entry.map_err(|e| {
            warn!(
                "Error reading directory: {}, error: {}",
                directory_path.to_str().unwrap(),
                e
            );
            Errors::FailToReadDatabaseDirectory
        })?;

        let name = entry.file_name();
        let filename = name.to_str().ok_or(Errors::FailToReadDatabaseDirectory)?;

        // filename is like 00000.bcdata
        if filename.ends_with(DATAFILE_NAME_SUFFIX) {
            let split_names: Vec<&str> = filename.split(DATAFILE_SEPARATOR).collect();
            let file_id = split_names[0].parse::<u32>().map_err(|e| {
                warn!("database directory may be corrupted: {}", e);
                Errors::DatabaseFileCorrupted
            })?;
            file_ids.push(file_id);
        }

        file_ids.sort();

        for fid in file_ids.iter() {
            let df = DataFile::new(directory_path.to_path_buf(), *fid)?;
            data_files.push(df);
        }
    }

    if data_files.is_empty() {
        info!("no datafile in directory, create a new one");
        let df = DataFile::new(directory_path.to_path_buf(), 0)?;
        data_files.push(df);
    }

    Ok(data_files)
}

// fn load_index(files: &vec![DataFile]) -> Indexer {}
