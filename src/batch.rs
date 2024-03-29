use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::{Bytes, BytesMut};
use log::error;
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
    data::log_record::{LogRecord, LogRecordKey, LogRecordType},
    db::Engine,
    error::{Errors, Result},
    options::WriteBatchOptions,
};

const TXN_FIN_PREFIX: &[u8] = "txn_fin_prefix".as_bytes();
pub(crate) const NON_TXN_PREFIX: &[u8] = "non_txn".as_bytes();

pub struct WriteBatch<'a> {
    engine: &'a Engine,
    options: WriteBatchOptions,
    pending_batch: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
}

impl Engine {
    pub fn write_batch(&self, options: &WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            engine: self,
            options: options.clone(),
            pending_batch: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let record = LogRecord {
            key: key.into(),
            value: value.into(),
            record_type: LogRecordType::Normal,
        };

        let mut lock_guard = self.pending_batch.lock();
        lock_guard.insert(key.into(), record);

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }

        let has_key = match self.engine.get(Bytes::copy_from_slice(key)) {
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
            .entry(key.into())
            .or_insert(LogRecord {
                key: key.into(),
                value: Default::default(),
                record_type: LogRecordType::Deleted,
            })
            .record_type
            == LogRecordType::Normal
        {
            lock_guard.remove(key);
            if has_key {
                lock_guard.insert(
                    key.into(),
                    LogRecord {
                        key: key.into(),
                        value: Default::default(),
                        record_type: LogRecordType::Deleted,
                    },
                );
            }
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let mut batch = self.pending_batch.lock();
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
                let original_key = &record.key;
                let record = LogRecord {
                    key: log_record_key_with_sequence(&record.key, prefix, seq_id)?,
                    value: record.value.clone(),
                    record_type: record.record_type,
                };
                let pos = self.engine.append_log_record(&record)?;
                prev.insert(pos, original_key);
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
            .try_for_each(|(pos, key)| -> Result<()> {
                match self.engine.indexer.put(key.clone(), pos) {
                    true => Ok(()),
                    false => Err(Errors::FailToUpdateIndex),
                }
            })?;

        batch.clear();
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let batch = self.pending_batch.lock();
        if let Some(not_commit) = batch.get(key) {
            match not_commit.record_type {
                LogRecordType::Normal => Ok(not_commit.value.clone()),
                LogRecordType::Deleted => Err(Errors::KeyNotFound),
                LogRecordType::BatchCommit => unreachable!(),
            }
        } else {
            self.engine
                .get(key.to_vec().into())
                .map(|value| value.into())
        }
    }
}

pub(crate) fn log_record_key_with_sequence(
    key: &[u8],
    prefix: &[u8],
    seq_id: usize,
) -> Result<Vec<u8>> {
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

pub(crate) fn log_record_key_parse(key: &[u8]) -> Result<LogRecordKey> {
    let mut buffer: BytesMut = key.into();
    let pos = decode_length_delimiter(&mut buffer).map_err(|e| {
        error!("decode log record with commit id failed: {}", e);
        Errors::DecodingError
    })?;
    let prefix = buffer.split_to(pos);

    let seq_id = decode_length_delimiter(&mut buffer).map_err(|e| {
        error!("decode log record with commit id failed: {}", e);
        Errors::DecodingError
    })?;

    Ok(LogRecordKey {
        prefix: prefix.into(),
        seq_id,
        key: buffer.into(),
    })
}

#[cfg(test)]
mod tests {

    use tempfile::Builder;

    use crate::{
        options::Options,
        utils::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    fn new_engine() -> (Engine, Options) {
        let mut opts = Options::default();
        opts.dir_path = Builder::new()
            .prefix("bitcast-rs")
            .tempdir()
            .unwrap()
            .path()
            .to_path_buf();
        opts.datafile_size = 64 * 1024 * 1024;

        return (
            Engine::open(opts.clone()).expect("failed to open engine"),
            opts,
        );
    }

    #[test]
    fn test_new_write_batch() {
        let (engine, _) = new_engine();
        assert_eq!(engine.write_batch(&Default::default()).is_ok(), true);
    }

    #[test]
    fn test_write_batch_not_commit() {
        let (engine, opts) = new_engine();

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");

        (0..10000).for_each(|i| {
            assert_eq!(
                write_batch.put(&get_test_key(i).to_vec(), &get_test_value(i).to_vec()),
                Ok(())
            );
            assert_eq!(engine.get(get_test_key(i).into()), Err(Errors::KeyNotFound));
            assert_eq!(
                write_batch.get(&get_test_key(i).to_vec()),
                Ok(get_test_value(i).to_vec())
            );
        });

        drop(write_batch);

        (0..10000).for_each(|i| {
            assert_eq!(engine.get(get_test_key(i).into()), Err(Errors::KeyNotFound));
        });

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        (0..10000).for_each(|i| {
            assert_eq!(engine.get(get_test_key(i).into()), Err(Errors::KeyNotFound));
        });
    }

    #[test]
    fn test_write_batch_put() {
        let (engine, opts) = new_engine();

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(101).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );

        assert_eq!(
            *write_batch.pending_batch.lock(),
            (101..102)
                .map(|x: usize| {
                    (
                        get_test_key(x).into(),
                        LogRecord {
                            key: get_test_key(x).into(),
                            value: get_test_value(x).into(),
                            record_type: LogRecordType::Normal,
                        },
                    )
                })
                .collect()
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );
        assert_eq!(write_batch.commit(), Ok(()));
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );
    }

    #[test]
    fn test_write_batch_put_and_update() {
        let (engine, opts) = new_engine();

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(101).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );

        assert_eq!(
            *write_batch.pending_batch.lock(),
            (101..102)
                .map(|x: usize| {
                    (
                        get_test_key(x).into(),
                        LogRecord {
                            key: get_test_key(x).into(),
                            value: get_test_value(x).into(),
                            record_type: LogRecordType::Normal,
                        },
                    )
                })
                .collect()
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );
        assert_eq!(write_batch.commit(), Ok(()));
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(102).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into()),
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(102).to_vec())
        );

        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(103).to_vec()),
            Ok(())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(103).to_vec())
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into()),
        );

        assert_eq!(write_batch.commit(), Ok(()));

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(103).into())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(103).to_vec())
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(103).into())
        );
    }

    #[test]
    fn test_write_batch_put_and_delete() {
        let (engine, opts) = new_engine();

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(101).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );

        assert_eq!(
            *write_batch.pending_batch.lock(),
            (101..102)
                .map(|x: usize| {
                    (
                        get_test_key(x).into(),
                        LogRecord {
                            key: get_test_key(x).into(),
                            value: get_test_value(x).into(),
                            record_type: LogRecordType::Normal,
                        },
                    )
                })
                .collect()
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );
        assert_eq!(write_batch.commit(), Ok(()));
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).to_vec())
        );
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(102).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into()),
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(102).to_vec())
        );

        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(103).to_vec()),
            Ok(())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(103).to_vec())
        );

        assert_eq!(write_batch.delete(&get_test_key(101).to_vec()), Ok(()));
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound)
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into()),
        );

        assert_eq!(write_batch.commit(), Ok(()));

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound)
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
    }

    #[test]
    fn test_write_batch_put_and_delete_with_no_batch_add() {
        let (engine, opts) = new_engine();

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(101).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).into()),
        );

        assert_eq!(
            engine.put(get_test_key(101).into(), get_test_value(201).into()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(201).into()),
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).into()),
        );

        assert_eq!(
            *write_batch.pending_batch.lock(),
            (101..102)
                .map(|x: usize| {
                    (
                        get_test_key(x).into(),
                        LogRecord {
                            key: get_test_key(x).into(),
                            value: get_test_value(x).into(),
                            record_type: LogRecordType::Normal,
                        },
                    )
                })
                .collect()
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(201).into()),
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).into()),
        );
        assert_eq!(write_batch.commit(), Ok(()));
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).into()),
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(101).into())
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(101).into()),
        );

        assert_eq!(engine.delete(get_test_key(101).into()), Ok(()));
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound),
        );

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound),
        );
        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(102).to_vec()),
            Ok(())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(102).into()),
        );
        assert_eq!(engine.delete(get_test_key(101).into()), Ok(()));
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(102).into()),
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound),
        );

        assert_eq!(
            write_batch.put(&get_test_key(101).to_vec(), &get_test_value(202).to_vec()),
            Ok(())
        );

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound),
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Ok(get_test_value(202).into()),
        );

        assert_eq!(write_batch.delete(&get_test_key(101).to_vec()), Ok(()));
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound),
        );

        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound),
        );

        assert_eq!(write_batch.commit(), Ok(()));

        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            write_batch.get(&get_test_key(101).to_vec()),
            Err(Errors::KeyNotFound),
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
    }

    #[test]
    fn test_simple_batch_commit_retrieve() {
        let (engine, opts) = new_engine();
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(engine.put(get_test_key(103), get_test_value(103)), Ok(()));

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");

        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1000)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1001)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1002)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(101), &get_test_value(1010)),
            Ok(())
        );

        assert_eq!(write_batch.delete(&get_test_key(103)), Ok(()));
        assert_eq!(write_batch.commit(), Ok(()));

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");

        assert_eq!(
            write_batch.put(&get_test_key(110), &get_test_value(1100)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1011)),
            Ok(())
        );
        assert_eq!(write_batch.commit(), Ok(()));

        assert_eq!(
            engine.get(get_test_key(100).into()),
            Ok(get_test_value(1011).into())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(1010).into())
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(100).into()),
            Ok(get_test_value(1011).into())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(1010).into())
        );
    }

    #[test]
    fn test_complex_batch_commit_retrieve() {
        let (engine, opts) = new_engine();
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Err(Errors::KeyNotFound)
        );

        (200..10000).for_each(|x| {
            assert_eq!(engine.put(get_test_key(x), get_test_value(x)), Ok(()));
        });

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");

        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1000)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1001)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1002)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(101), &get_test_value(1010)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(102), &get_test_value(1020)),
            Ok(())
        );
        assert_eq!(write_batch.delete(&get_test_key(203)), Ok(()));
        assert_eq!(write_batch.commit(), Ok(()));

        let mut write_batch = engine
            .write_batch(&Default::default())
            .expect("failed to create write batch");

        assert_eq!(
            write_batch.put(&get_test_key(110), &get_test_value(1100)),
            Ok(())
        );
        assert_eq!(
            write_batch.put(&get_test_key(100), &get_test_value(1011)),
            Ok(())
        );

        assert_eq!(write_batch.delete(&get_test_key(280)), Ok(()));
        assert_eq!(write_batch.delete(&get_test_key(102)), Ok(()));
        assert_eq!(write_batch.commit(), Ok(()));

        assert_eq!(
            engine.get(get_test_key(100).into()),
            Ok(get_test_value(1011).into())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(1010).into())
        );
        assert_eq!(
            engine.get(get_test_key(102).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            engine.get(get_test_key(110).into()),
            Ok(get_test_value(1100).into())
        );
        assert_eq!(
            engine.get(get_test_key(280).into()),
            Err(Errors::KeyNotFound)
        );

        assert_eq!(engine.close(), Ok(()));
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        assert_eq!(
            engine.get(get_test_key(100).into()),
            Ok(get_test_value(1011).into())
        );
        assert_eq!(
            engine.get(get_test_key(101).into()),
            Ok(get_test_value(1010).into())
        );
        assert_eq!(
            engine.get(get_test_key(102).into()),
            Err(Errors::KeyNotFound)
        );
        assert_eq!(
            engine.get(get_test_key(110).into()),
            Ok(get_test_value(1100).into())
        );
        assert_eq!(
            engine.get(get_test_key(280).into()),
            Err(Errors::KeyNotFound)
        );
    }

    #[test]
    fn test_log_record_key_with_sequence() {
        let serialized_key = log_record_key_with_sequence(
            &get_test_key(101).to_vec(),
            &get_test_key(201).to_vec(),
            89,
        )
        .expect("serialization failed");

        assert_eq!(
            log_record_key_parse(&serialized_key),
            Ok(LogRecordKey {
                prefix: get_test_key(201).into(),
                seq_id: 89,
                key: get_test_key(101).into(),
            })
        );
    }
}
