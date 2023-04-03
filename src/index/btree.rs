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
        self.pos = match self.items.binary_search_by(|(x, _)| {
            let order = x.cmp(&key.into());
            if self.options.reverse {
                order.reverse()
            } else {
                order
            }
        }) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.pos >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.pos) {
            if item.0.starts_with(&self.options.prefix) {
                return Some((&item.0, &item.1));
            }
            self.pos += 1;
        }

        None
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
}
