use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    error::{Errors, Result},
    options::WriteBatchOptions,
};

pub struct WriteBatch<'a> {
    engine: &'a mut Engine,
    options: WriteBatchOptions,
    pending_batch: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
}

impl Engine {
    pub fn write_batch(&mut self, options: &WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            engine: self,
            options: options.clone(),
            pending_batch: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record = LogRecord {
            key: key.clone(),
            value,
            record_type: LogRecordType::NORAML,
        };

        let mut lock_guard = self.pending_batch.lock();
        lock_guard.insert(key, record);

        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let has_key = match self.engine.get(Bytes::copy_from_slice(&key)) {
            Ok(_) => Ok(true),
            Err(err) => {
                if err == Errors::KeyNotFound {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }?;

        let mut lock_guard = self.pending_batch.lock();
        if lock_guard
            .entry(key.clone())
            .or_insert(LogRecord {
                key: key.clone(),
                value: Default::default(),
                record_type: LogRecordType::DELETED,
            })
            .record_type
            == LogRecordType::NORAML
        {
            lock_guard.remove(&key);
            if has_key {
                lock_guard.insert(
                    key.clone(),
                    LogRecord {
                        key: key.clone(),
                        value: Default::default(),
                        record_type: LogRecordType::DELETED,
                    },
                );
            }
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let batch = self.pending_batch.lock();
        if batch.is_empty() {
            return Ok(());
        }

        if batch.len() > self.options.max_batch_size {
            return Err(Errors::ExceedBatchMaxSize);
        }

        let seq_id = self.engine.batch_commit_id.fetch_add(1, Ordering::SeqCst);
        let commit_lock = self.engine.batch_commit_lock.lock();

        // TODO: add log record batch with seq_id to engine and set a finish flag in end

        todo!()
    }
}

fn log_record_key_with_sequence(key: &[u8], seq_id: usize) -> (Vec<u8>, usize) {
    // TODO: implement it
    todo!()
}
