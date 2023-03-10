use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::data::log_record_pos::LogRecordPos;

use super::index::Indexer;

pub struct BTreeIndexer {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_add() {
        let mut bt = BTreeIndexer::new();

        assert_eq!(
            bt.put(
                "".as_bytes().to_vec(),
                LogRecordPos {
                    file_id: 1,
                    offset: 122,
                },
            ),
            true,
        );

        assert_eq!(
            bt.put(
                "".as_bytes().to_vec(),
                LogRecordPos {
                    file_id: 1121,
                    offset: 44,
                },
            ),
            true,
        );

        assert_eq!(
            bt.put(
                "sadsad".as_bytes().to_vec(),
                LogRecordPos {
                    file_id: 0,
                    offset: 0,
                },
            ),
            true,
        );
        assert_eq!(
            bt.put(
                "ssaaa".as_bytes().to_vec(),
                LogRecordPos {
                    file_id: 2131,
                    offset: 11122,
                },
            ),
            true,
        );

        assert_eq!(
            bt.put(
                vec![1, 2, 3],
                LogRecordPos {
                    file_id: 1223,
                    offset: 1223141,
                },
            ),
            true,
        );

        assert_eq!(
            bt.put(
                vec![],
                LogRecordPos {
                    file_id: 1,
                    offset: 122,
                },
            ),
            true,
        );
    }

    #[test]
    fn btree_test_get() {
        let bt = BTreeIndexer::new();
        let res = bt.put(
            "\0".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 0,
                offset: 88,
            },
        );
        assert_eq!(res, true);
        assert_eq!(
            bt.get("\0".as_bytes().to_vec()),
            Some(LogRecordPos {
                file_id: 0,
                offset: 88,
            }),
        );
    }
}
