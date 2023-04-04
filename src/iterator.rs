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

    pub fn seek(&mut self, key: Bytes) {
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

    #[test]
    fn test_iterator_new() {
        // todo!()
    }

    #[test]
    fn test_iterator_rewind() {
        // todo!()
    }

    #[test]
    fn test_iterator_seek() {
        // todo!()
    }

    #[test]
    fn test_iterator_next() {
        // todo!()
    }
}
