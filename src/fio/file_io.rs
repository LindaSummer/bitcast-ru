use std::{
    fs::File,
    io::{Read, Write},
    os::unix::prelude::FileExt,
    sync::Arc,
};

use log::error;
use parking_lot::RwLock;

use super::io_manager::IOManager;
use crate::error::{Errors, Result};

/// standard system io
pub struct FileIO {
    fd: Arc<RwLock<File>>, // file descriptor
}

impl IOManager for FileIO {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        read_guard.read_at(buf, offset).map_err(|e| {
            error!("read data file failed: {:?}", e);
            Errors::FailToReadFromDataFile(e)
        })
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        write_guard.write(buf).map_err(|e| {
            error!("write data file failed: {:?}", e);
            Errors::FailToWriteToDataFile()
        })
    }

    fn sync(&mut self) -> Result<()> {
        todo!()
    }

    fn close(&mut self) -> Result<()> {
        todo!()
    }
}
