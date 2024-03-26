use std::{fs, ops::Deref, path::PathBuf};

use log::error;

use crate::{
    batch::{log_record_key_parse, log_record_key_with_sequence, NON_TXN_PREFIX},
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
        merge_flag_data_file::{HINT_FILE_NAME, MERGE_FLAG_FILE_NAME},
    },
    db::{Engine, NON_BATCH_COMMIT_ID},
    error::{Errors, Result},
    options::Options,
};

const MERGE_DIR_NAME: &'static str = "_merge";
const MERGE_FIN_KEY: &'static [u8] = b"fin";

impl Engine {
    pub(crate) fn merge(&self) -> Result<()> {
        let _lock = self.merge_lock.lock();
        let old_files = self.old_files.read();
        self.merge_lock
            .try_lock()
            .map(|_| -> Result<_> {
                let current_files =
                    self.rotate_merge_file()?
                        .iter()
                        .try_fold(Vec::new(), |mut acc, id| match old_files.get(id) {
                            Some(f) => {
                                acc.push(f);
                                Ok(acc)
                            }
                            None => Err(Errors::DataFileNotFound),
                        })?;
                let merge_dir = self.create_merge_dir()?;
                let mut _engine = Self::open(Options {
                    dir_path: merge_dir,
                    ..self.options.deref().clone()
                })?;
                self.migrate_data(&mut _engine, &current_files)
            })
            .ok_or(Errors::MergeInProgress)
            .and_then(|r| r)
    }

    fn migrate_data(
        &self,
        merge_db: &mut Engine,
        files_to_merge_from: &Vec<&DataFile>,
    ) -> Result<()> {
        if files_to_merge_from.is_empty() {
            return Ok(());
        }
        let dir_path = &self.options.dir_path;
        let mut hint_file = DataFile::new_hint_file(&dir_path.join(HINT_FILE_NAME))?;
        // convert Option to Result
        let last_merge_file_id = files_to_merge_from
            .iter()
            .map(|&file| file.file_id())
            .max()
            .ok_or(Errors::DatabaseFileCorrupted)?;
        files_to_merge_from
            .iter()
            .try_for_each(|&file| -> Result<_> {
                let mut offset = 0;
                loop {
                    match file.read_log_record(offset) {
                        Ok(record) => {
                            let result = {
                                let record = &record.record;
                                let key = log_record_key_parse(&record.key)?.key;
                                if self.indexer.get(key.clone())
                                    == Some(LogRecordPos {
                                        file_id: file.file_id(),
                                        offset,
                                    })
                                {
                                    merge_db
                                        .append_log_record(&LogRecord {
                                            key: log_record_key_with_sequence(
                                                &key,
                                                NON_TXN_PREFIX,
                                                NON_BATCH_COMMIT_ID,
                                            )?
                                            .into(),
                                            value: record.value.clone().into(),
                                            record_type: LogRecordType::Normal,
                                        })
                                        .and_then(|log_record_pos| -> Result<()> {
                                            hint_file
                                                .write_hint_record(&key, &log_record_pos)
                                                .map(|_| ())
                                        })?
                                }
                            };
                            offset += record.size;
                            result
                        }
                        Err(Errors::ReadEOF) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
            })
            .and_then(|_| hint_file.sync())
            .and_then(|_| merge_db.sync())
            .and_then(|_| -> Result<()> {
                DataFile::new_merge_fin_file(dir_path).map(|mut fin_file| -> Result<_> {
                    fin_file.write(
                        &LogRecord {
                            key: MERGE_FIN_KEY.to_vec(),
                            value: (last_merge_file_id + 1).to_string().into_bytes(),
                            record_type: LogRecordType::Normal,
                        }
                        .encode(),
                    )?;
                    fin_file.sync()
                })?
            })
    }

    pub(crate) fn rotate_merge_file(&self) -> Result<Vec<u32>> {
        let mut active_file = self.active_file.write();
        let active_id = self.rotate_active_file(&mut active_file)?;
        Ok(self
            .old_files
            .read()
            .keys()
            .filter(|&id| *id <= active_id)
            .cloned()
            .collect::<Vec<u32>>())
    }

    pub(crate) fn create_merge_dir(&self) -> Result<PathBuf> {
        let merge_dir = self.options.dir_path.join(MERGE_DIR_NAME);
        if merge_dir.exists() {
            let merge_flg_file = merge_dir.join(MERGE_FLAG_FILE_NAME);
            if merge_flg_file.exists() {
                // TODO: remove old files in flg file
                self.remove_old_files(&merge_flg_file)?;
            } else {
                fs::create_dir_all(&merge_dir).map_err(|e| -> Errors {
                    error!("fail to create merge dir: {:?}", e);
                    Errors::FailToCreateDatabaseDirectory
                })?;
            }
        } else {
            fs::create_dir_all(&merge_dir).map_err(|e| -> Errors {
                error!("fail to create merge dir: {:?}", e);
                Errors::FailToCreateDatabaseDirectory
            })?;
        }
        Ok(merge_dir)
    }

    fn remove_old_files(&self, merge_flag_file: &PathBuf) -> Result<()> {
        todo!()
    }
}
