use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::{Bytes, BytesMut};
use log::error;
use parking_lot::Mutex;
use prost::encode_length_delimiter;

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    error::{Errors, Result},
    options::WriteBatchOptions,
};

const TXN_FIN_PREFIX: &[u8] = "txn_fin_prefix".as_bytes();

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
            record_type: LogRecordType::Normal,
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
                record_type: LogRecordType::Deleted,
            })
            .record_type
            == LogRecordType::Normal
        {
            lock_guard.remove(&key);
            if has_key {
                lock_guard.insert(
                    key.clone(),
                    LogRecord {
                        key,
                        value: Default::default(),
                        record_type: LogRecordType::Deleted,
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
        let prefix = &self.engine.batch_prefix;
        let _commit_lock = self.engine.batch_commit_lock.lock();

        let record_pos = batch
            .values()
            .try_fold(HashMap::new(), |mut prev, record| {
                let record = LogRecord {
                    key: log_record_key_with_sequence(&record.key, prefix, seq_id)?,
                    value: record.value.clone(),
                    record_type: record.record_type,
                };
                let pos = self.engine.append_log_record(&record)?;
                prev.insert(pos, record);
                Ok(prev)
            })?;

        self.engine.append_log_record(&LogRecord {
            key: log_record_key_with_sequence(TXN_FIN_PREFIX, prefix, seq_id)?,
            value: Default::default(),
            record_type: LogRecordType::BatchCommit,
        })?;

        // update index

        record_pos
            .into_iter()
            .try_for_each(|(pos, record)| -> Result<()> {
                match self.engine.indexer.put(record.key, pos) {
                    true => Ok(()),
                    false => Err(Errors::FailToUpdateIndex),
                }
            })
    }
}

fn log_record_key_with_sequence(key: &[u8], prefix: &[u8], seq_id: usize) -> Result<Vec<u8>> {
    let mut buffer = BytesMut::new();
    encode_length_delimiter(prefix.len(), &mut buffer).map_err(|e| {
        error!("encode batch record failed: {}", e);
        Errors::EncodingError
    })?;
    buffer.extend_from_slice(prefix);
    encode_length_delimiter(seq_id, &mut buffer).map_err(|e| {
        error!("encode batch record failed: {}", e);
        Errors::EncodingError
    })?;
    buffer.extend_from_slice(key);
    Ok(buffer.into())
}
