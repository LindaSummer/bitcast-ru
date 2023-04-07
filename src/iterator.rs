use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    db::Engine, error::Result, index::indexer::IndexIterator, options::IndexIteratorOptions,
};

pub struct Iterator<'a> {
    index_iterator: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl<'a> Iterator<'a> {
    pub(crate) fn new(index_iterator: Box<dyn IndexIterator>, engine: &'a Engine) -> Self {
        Self {
            index_iterator: Arc::new(RwLock::new(index_iterator)),
            engine,
        }
    }

    pub fn rewind(&self) {
        self.index_iterator.write().rewind();
    }

    pub fn seek(&self, key: Bytes) {
        self.index_iterator.write().seek(key.as_ref())
    }

    pub fn next(&self) -> Result<Option<(Bytes, Bytes)>> {
        let (key, pos) = match self.index_iterator.write().next() {
            Some((key, pos)) => (key.clone(), *pos),
            None => {
                return Ok(None);
            }
        };

        let value = self.engine.get_by_position(&pos)?;

        Ok(Some((key.into(), value)))
    }
}

impl Engine {
    pub fn iterator(&self, options: IndexIteratorOptions) -> Iterator {
        Iterator::new(self.indexer.iterator(options), self)
    }
}

#[cfg(test)]
mod tests {
    // use super::*;S

    use tempfile::Builder;

    use crate::{
        db::Engine,
        options::{IndexIteratorOptions, Options},
    };

    #[test]
    fn test_iterator_rewind() {
        let mut opts = Options::default();
        opts.dir_path = Builder::new()
            .prefix("bitcast-rs")
            .tempdir()
            .unwrap()
            .path()
            .to_path_buf();
        opts.datafile_size = 64 * 1024 * 1024;

        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let iterator_options = IndexIteratorOptions::default();
        let iterator = engine.iterator(iterator_options);
        assert_eq!(iterator.next(), Ok(None));
        iterator.rewind();
        assert_eq!(iterator.next(), Ok(None));

        assert_eq!(engine.put("key".into(), "value".into()), Ok(()));
        let iterator_options = IndexIteratorOptions::default();
        let iterator = engine.iterator(iterator_options);
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));
        iterator.rewind();
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));
    }

    #[test]
    fn test_iterator_seek_next() {
        let mut opts = Options::default();
        opts.dir_path = Builder::new()
            .prefix("bitcast-rs")
            .tempdir()
            .unwrap()
            .path()
            .to_path_buf();
        opts.datafile_size = 64 * 1024 * 1024;

        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let iterator_options = IndexIteratorOptions::default();
        let iterator = engine.iterator(iterator_options);
        assert_eq!(iterator.next(), Ok(None));
        iterator.rewind();
        assert_eq!(iterator.next(), Ok(None));

        assert_eq!(engine.put("key".into(), "value".into()), Ok(()));
        assert_eq!(engine.put("key1".into(), "value1".into()), Ok(()));
        let iterator_options = IndexIteratorOptions::default();
        let iterator = engine.iterator(iterator_options);
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("kex".into());
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("key".into());
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("key1".into());
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("key2".into());
        assert_eq!(iterator.next(), Ok(None));

        let iterator_options = IndexIteratorOptions {
            prefix: Default::default(),
            reverse: true,
        };
        let iterator = engine.iterator(iterator_options);
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.rewind();
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("key2".into());
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("key1".into());
        assert_eq!(iterator.next(), Ok(Some(("key1".into(), "value1".into()))));
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("key0".into());
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("key".into());
        assert_eq!(iterator.next(), Ok(Some(("key".into(), "value".into()))));
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("kex".into());
        assert_eq!(iterator.next(), Ok(None));
    }

    #[test]
    fn test_iterator_seek_prefix_next() {
        let mut opts = Options::default();
        opts.dir_path = Builder::new()
            .prefix("bitcast-rs")
            .tempdir()
            .unwrap()
            .path()
            .to_path_buf();
        opts.datafile_size = 64 * 1024 * 1024;

        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let iterator_options = IndexIteratorOptions {
            prefix: "prefix_".into(),
            reverse: false,
        };
        let iterator = engine.iterator(iterator_options.clone());
        assert_eq!(iterator.next(), Ok(None));
        iterator.rewind();
        assert_eq!(iterator.next(), Ok(None));

        assert_eq!(engine.put("key".into(), "value".into()), Ok(()));
        assert_eq!(engine.put("key1".into(), "value1".into()), Ok(()));
        assert_eq!(engine.put("prefix_key".into(), "value".into()), Ok(()));
        assert_eq!(engine.put("prefix_key1".into(), "value1".into()), Ok(()));

        let iterator = engine.iterator(iterator_options.clone());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("prefix_kex".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("prefix_key".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("prefix_key1".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(iterator.next(), Ok(None));
        iterator.seek("prefix_key2".into());
        assert_eq!(iterator.next(), Ok(None));

        let iterator_options = IndexIteratorOptions {
            prefix: "prefix_".into(),
            reverse: true,
        };
        let iterator = engine.iterator(iterator_options);
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.rewind();
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("prefix_key2".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("prefix_key1".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key1".into(), "value1".into())))
        );
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("prefix_key0".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("prefix_key".into());
        assert_eq!(
            iterator.next(),
            Ok(Some(("prefix_key".into(), "value".into())))
        );
        assert_eq!(iterator.next(), Ok(None));

        iterator.seek("prefix_kex".into());
        assert_eq!(iterator.next(), Ok(None));
    }
}
