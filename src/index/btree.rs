use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::{data::log_record::LogRecordPos, options::IndexIteratorOptions};

use super::indexer::{IndexIterator, Indexer};

pub struct BTreeIndexer {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Default for BTreeIndexer {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeIndexer {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTreeIndexer {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key).is_some()
    }

    fn iterator(&self, options: IndexIteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = self
            .tree
            .read()
            .iter()
            .filter(|&(key, _)| key.starts_with(&options.prefix))
            .map(|(key, value)| (key.clone(), *value))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse();
        }
        Box::new(BtreeIndexIterator {
            items,
            pos: 0,
            options,
        })
    }
}

struct BtreeIndexIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    pos: usize,
    options: IndexIteratorOptions,
}

impl IndexIterator for BtreeIndexIterator {
    fn rewind(&mut self) {
        self.pos = 0;
    }

    fn seek(&mut self, key: &[u8]) {
        // let key: Vec<u8> = self
        //     .options
        //     .prefix
        //     .iter()
        //     .chain(key.iter())
        //     .cloned()
        //     .collect();

        self.pos = match self.items.binary_search_by(|(x, _)| {
            let order = x.as_slice().cmp(key);
            // let order = key.cmp(x.as_slice());
            if self.options.reverse {
                order.reverse()
            } else {
                order
            }
        }) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.pos >= self.items.len() {
            return None;
        }
        // while let Some(item) = self.items.get(self.pos) {
        //     self.pos += 1;
        //     if item.0.starts_with(&self.options.prefix) {
        //         return Some((&item.0, &item.1));
        //     }
        // }

        let item = self.items.get(self.pos).map(|x| (&x.0, &x.1));
        self.pos += 1;
        item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_add() {
        let bt = BTreeIndexer::new();

        assert!(bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 122,
            },
        ));

        assert!(bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1121,
                offset: 44,
            },
        ));

        assert!(bt.put(
            "sadsad".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 0,
            },
        ));

        assert!(bt.put(
            "ssaaa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2131,
                offset: 11122,
            },
        ));

        assert!(bt.put(
            vec![1, 2, 3],
            LogRecordPos {
                file_id: 1223,
                offset: 1223141,
            },
        ));

        assert!(bt.put(
            vec![],
            LogRecordPos {
                file_id: 1,
                offset: 122,
            },
        ));
    }

    #[test]
    fn btree_test_get() {
        let bt = BTreeIndexer::new();

        assert_eq!(bt.get("\0".as_bytes().to_vec()), None);

        let res = bt.put(
            "\0".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 88,
            },
        );
        assert!(res);
        assert_eq!(
            bt.get("\0".as_bytes().to_vec()),
            Some(LogRecordPos {
                file_id: 0,
                offset: 88,
            }),
        );

        let res = bt.put(
            vec![],
            LogRecordPos {
                file_id: 0,
                offset: 881,
            },
        );

        assert_eq!(res, true);
        assert_eq!(
            bt.get(vec![]),
            Some(LogRecordPos {
                file_id: 0,
                offset: 881,
            }),
        );

        let res = bt.put(
            vec![],
            LogRecordPos {
                file_id: 213123,
                offset: 88222,
            },
        );

        assert!(res);
        assert_eq!(
            bt.get(vec![]),
            Some(LogRecordPos {
                file_id: 213123,
                offset: 88222,
            }),
        );
    }

    #[test]
    fn test_bt_delete() {
        let bt = BTreeIndexer::new();

        assert!(!bt.delete("test-key".as_bytes().to_vec()));

        assert!(bt.put(
            "test-key".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 122,
                offset: 881
            }
        ));

        assert_eq!(
            bt.get("test-key".as_bytes().to_vec()),
            Some(LogRecordPos {
                file_id: 122,
                offset: 881
            }),
        );

        assert!(bt.delete("test-key".as_bytes().to_vec()));
        assert!(!bt.delete("test-key".as_bytes().to_vec()));
    }

    #[test]
    fn test_iterator_seek() {
        // no record
        let indexer = BTreeIndexer::new();
        let mut iterator = indexer.iterator(Default::default());

        iterator.seek("some_key".as_bytes());
        assert_eq!(iterator.next(), None);

        // only one record
        let indexer = BTreeIndexer::new();
        assert!(indexer.put(
            "0a".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        ));
        let mut iterator = indexer.iterator(Default::default());
        iterator.seek("1".as_bytes());
        assert_eq!(iterator.next(), None);
        let mut iterator = indexer.iterator(Default::default());
        iterator.seek("0".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"0a".as_bytes().into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        // many records
        let indexer = BTreeIndexer::new();
        assert!(indexer.put(
            "0a".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        ));
        assert!(indexer.put(
            "0b".as_bytes().into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        ));
        assert!(indexer.put(
            "1c".as_bytes().into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        ));
        let mut iterator = indexer.iterator(Default::default());
        iterator.seek("2".as_bytes());
        assert_eq!(iterator.next(), None);
        iterator.seek("1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"1c".as_bytes().into(),
                &LogRecordPos {
                    file_id: 3,
                    offset: 3,
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("0".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"0a".as_bytes().into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1,
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"0b".as_bytes().into(),
                &LogRecordPos {
                    file_id: 2,
                    offset: 2,
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"1c".as_bytes().into(),
                &LogRecordPos {
                    file_id: 3,
                    offset: 3,
                }
            ))
        );
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_iterator_seek_reverse() {
        let options = IndexIteratorOptions {
            prefix: Default::default(),
            reverse: true,
        };

        // no record
        let indexer = BTreeIndexer::new();
        let mut iterator = indexer.iterator(options.clone());

        iterator.seek("some_key".as_bytes());
        assert_eq!(iterator.next(), None);

        // only one record
        let indexer = BTreeIndexer::new();
        assert!(indexer.put(
            "0a".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        ));
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("0".as_bytes());
        assert_eq!(iterator.next(), None);
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"0a".as_bytes().into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        // many records
        let indexer = BTreeIndexer::new();
        assert!(indexer.put(
            "0a".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        ));
        assert!(indexer.put(
            "0b".as_bytes().into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        ));
        assert!(indexer.put(
            "1c".as_bytes().into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        ));
        let mut iterator = indexer.iterator(options);
        iterator.seek("0".as_bytes());
        assert_eq!(iterator.next(), None);
        iterator.seek("1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"0b".as_bytes().into(),
                &LogRecordPos {
                    file_id: 2,
                    offset: 2,
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"0a".as_bytes().into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1,
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("2".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"1c".as_bytes().into(),
                &LogRecordPos {
                    file_id: 3,
                    offset: 3,
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"0b".as_bytes().into(),
                &LogRecordPos {
                    file_id: 2,
                    offset: 2,
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"0a".as_bytes().into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1,
                }
            ))
        );
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_seek_with_prefix() {
        let options = IndexIteratorOptions {
            prefix: "prefix_".into(),
            reverse: false,
        };

        let indexer = BTreeIndexer::new();
        // no record
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("mykey".as_bytes());
        assert_eq!(iterator.next(), None);

        // record with prefix missing
        assert!(indexer.put(
            "some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        iterator.seek("some_key".as_bytes());
        assert_eq!(iterator.next(), None);

        let indexer = BTreeIndexer::new();
        // record with prefix hint
        assert!(indexer.put(
            "prefix_some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("prefix_".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key".into(),
                &LogRecordPos {
                    file_id: 202,
                    offset: 202
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_key_1".as_bytes());
        assert_eq!(iterator.next(), None);

        // records with more than one hint
        let indexer = BTreeIndexer::new();
        // record with prefix hint
        assert!(indexer.put(
            "prefix_some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        assert!(indexer.put(
            "prefix_some_key_1".into(),
            LogRecordPos {
                file_id: 209,
                offset: 209,
            },
        ));
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("prefix_".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key".into(),
                &LogRecordPos {
                    file_id: 202,
                    offset: 202
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key_1".into(),
                &LogRecordPos {
                    file_id: 209,
                    offset: 209
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_key_1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key_1".into(),
                &LogRecordPos {
                    file_id: 209,
                    offset: 209
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_key_2".as_bytes());
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_seek_reverse_with_prefix() {
        let options = IndexIteratorOptions {
            prefix: "prefix_".into(),
            reverse: true,
        };

        let indexer = BTreeIndexer::new();
        // no record
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("mykey".as_bytes());
        assert_eq!(iterator.next(), None);

        // record with prefix missing
        assert!(indexer.put(
            "some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        iterator.seek("some_key".as_bytes());
        assert_eq!(iterator.next(), None);

        let indexer = BTreeIndexer::new();
        // record with prefix hint
        assert!(indexer.put(
            "prefix_some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("prefix_".as_bytes());
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_key_1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key".into(),
                &LogRecordPos {
                    file_id: 202,
                    offset: 202
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        // records with more than one hint
        let indexer = BTreeIndexer::new();
        // record with prefix hint
        assert!(indexer.put(
            "prefix_some_key".into(),
            LogRecordPos {
                file_id: 202,
                offset: 202,
            },
        ));
        assert!(indexer.put(
            "prefix_some_key_1".into(),
            LogRecordPos {
                file_id: 209,
                offset: 209,
            },
        ));
        let mut iterator = indexer.iterator(options.clone());
        iterator.seek("prefix_".as_bytes());
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_key_1".as_bytes());
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key_1".into(),
                &LogRecordPos {
                    file_id: 209,
                    offset: 209
                }
            ))
        );
        assert_eq!(
            iterator.next(),
            Some((
                &"prefix_some_key".into(),
                &LogRecordPos {
                    file_id: 202,
                    offset: 202
                }
            ))
        );
        assert_eq!(iterator.next(), None);

        iterator.seek("prefix_some_kex".as_bytes());
        assert_eq!(iterator.next(), None);
    }
}
