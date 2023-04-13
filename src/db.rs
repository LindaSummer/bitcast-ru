use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    fs,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};
use ulid::Ulid;

use crate::{
    data::{
        data_file::{DataFile, DATAFILE_NAME_SUFFIX, DATAFILE_SEPARATOR},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Errors, Result},
    index::{self, indexer::new_indexer},
    options::Options,
};

const INITAIL_FILE_ID: u32 = 0;

pub struct Engine {
    options: Arc<Options>,

    active_file: Arc<RwLock<DataFile>>, // current active file
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old files
    pub(crate) indexer: Box<dyn index::Indexer>, // memory index manager

    file_ids: Vec<u32>, // file id list, only use in database initialize

    pub(crate) batch_commit_lock: Mutex<()>, // batch commit global lock
    pub(crate) batch_prefix: Vec<u8>,
    pub(crate) batch_commit_id: Arc<AtomicUsize>, // latest batch commit id
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _: Result<_> = self.close();
    }
}

impl Engine {
    pub fn open(opt: Options) -> Result<Self> {
        check_options(&opt)?;

        let dir_path = opt.clone().dir_path;
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).map_err(|e| {
                warn!("create database directory failed, error: {}", e);
                Errors::FailToCreateDatabaseDirectory
            })?;
        }

        let mut data_files = load_datafiles(&dir_path)?;
        let fids = data_files.iter().map(|f| f.file_id()).collect();
        let active_file = data_files.pop().ok_or(Errors::DataFileNotFound)?;
        let old_files = data_files
            .into_iter()
            .map(|f| (f.file_id(), f))
            .collect::<HashMap<_, _>>();

        let indexer = Box::new(new_indexer(opt.index_type.clone()));

        let ulid = Ulid::new();

        let mut engine = Engine {
            options: Arc::new(opt),
            active_file: Arc::new(RwLock::new(active_file)),
            indexer,
            old_files: Arc::new(RwLock::new(old_files)),
            file_ids: fids,
            batch_commit_lock: Default::default(),
            batch_prefix: ulid.to_string().into_bytes(), // TODO: make it generated from a distributed system
            batch_commit_id: Default::default(),         // TODO: create a persistent sequence id
        };
        engine.load_index_from_data_files()?;

        Ok(engine)
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
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

        self.get_by_position(&record_pos)
    }

    pub(crate) fn get_by_position(&self, pos: &LogRecordPos) -> Result<Bytes> {
        let mut active_file = self.active_file.read();
        let old_files = self.old_files.read();
        let hint_file = match active_file.file_id() == pos.file_id {
            true => {
                drop(old_files);
                active_file.borrow_mut()
            }
            false => {
                drop(active_file);
                old_files
                    .get(pos.file_id.borrow())
                    .ok_or(Errors::DataFileNotFound)?
            }
        };

        let record = hint_file.read_log_record(pos.offset)?;
        if record.record.record_type == LogRecordType::Deleted {
            Err(Errors::KeyNotFound)
        } else {
            Ok(record.record.value.into())
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
    pub(crate) fn append_log_record(&self, record: &LogRecord) -> Result<LogRecordPos> {
        let encode_log = record.encode();
        let mut active_file = self.active_file.write();
        if active_file.get_offset() + encode_log.len() as u64 > self.options.datafile_size {
            active_file.sync()?;
            // let prev_active_file =
            //     DataFile::new(self.options.dir_path.clone(), active_file.file_id())?;
            let mut old_files = self.old_files.write();
            let mut tmp_active_file =
                DataFile::new(self.options.dir_path.borrow(), active_file.file_id() + 1)?;
            std::mem::swap(&mut *active_file, &mut tmp_active_file);
            old_files.insert(tmp_active_file.file_id(), tmp_active_file);
        }
        let offset = active_file.get_offset();
        active_file.write(&encode_log)?;

        if self.options.sync_in_write {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.file_id(),
            offset,
        })
    }

    fn load_index_from_data_files(&mut self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }

        let mut active_file = self.active_file.write();
        let old_files = self.old_files.read();

        for (i, fid) in self.file_ids.iter().enumerate() {
            let mut offset: u64 = 0;
            loop {
                let data_file = if *fid == active_file.file_id() {
                    &*active_file
                } else {
                    old_files
                        .get(fid)
                        .ok_or(Errors::FailToReadDatabaseDirectory)?
                };

                let (log_record, size) = match data_file.read_log_record(offset) {
                    Ok(res) => Ok((res.record, res.size)),
                    Err(e) => {
                        if e == Errors::ReadEOF {
                            break;
                        };
                        Err(e)
                    }
                }?;
                if !match log_record.record_type {
                    // TODO: update data loading for batch commit
                    LogRecordType::Normal => {
                        let key = log_record.key.to_vec();
                        self.indexer.put(
                            key,
                            LogRecordPos {
                                file_id: *fid,
                                offset,
                            },
                        )
                    }
                    LogRecordType::Deleted => {
                        let key = log_record.key.to_vec();
                        self.indexer.delete(key)
                    }
                    LogRecordType::BatchCommit => todo!(),
                } {
                    error!("failed to update index");
                    return Err(Errors::FailToReadDatabaseDirectory);
                }
                offset += size;
            }

            if i == self.file_ids.len() - 1 {
                active_file.set_offset(offset);
            }
        }

        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        match self.indexer.get(key.to_vec()) {
            Some(_) => {
                let record = LogRecord {
                    key: key.to_vec(),
                    value: Default::default(),
                    record_type: LogRecordType::Deleted,
                };
                self.append_log_record(&record).map(|_| ())?;
                match self.indexer.delete(key.to_vec()) {
                    true => Ok(()),
                    false => {
                        warn!("delete key in indexer failed: {:?}", key);
                        Err(Errors::FailToUpdateIndex)
                    }
                }
            }
            None => Ok(()),
        }
    }

    pub fn close(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn list_keys(&self) -> Vec<Bytes> {
        self.indexer.list_keys()
    }

    pub fn fold<F>(&self, mut f: F) -> Result<()>
    where
        Self: Sized,
        F: FnMut(Bytes, Bytes) -> bool,
    {
        let iterator = self.iterator(Default::default());
        while let Ok(Some((key, value))) = iterator.next() {
            if !f(key, value) {
                return Ok(());
            }
        }
        Ok(())
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
    }

    file_ids.sort();

    for fid in file_ids.iter() {
        let df = DataFile::new(directory_path, *fid)?;
        data_files.push(df);
    }

    if data_files.is_empty() {
        info!("no datafile in directory, create a new one");
        let df = DataFile::new(directory_path, INITAIL_FILE_ID)?;
        data_files.push(df);
    }

    Ok(data_files)
}

// fn load_index(files: &vec![DataFile]) -> Indexer {}
