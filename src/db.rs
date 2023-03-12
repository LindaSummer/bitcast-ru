use bytes::Bytes;

use crate::error::{Errors, Result};

pub struct Engine {}

impl Engine {
    pub fn put(&self, key: &Bytes, value: &Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::EmptyKey);
        }
    }

    // pub fn get(&self, key: &Bytes) -> Result<Bytes> {}
}
