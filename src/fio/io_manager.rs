use std::path::PathBuf;

use crate::error::Result;

use super::file_io::FileIO;

/// IOManager provide a abstract interface for io manuplation
pub trait IOManager {
    /// read from @offset of a file
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// write buffer to a file
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// flush data to consistant file
    fn sync(&mut self) -> Result<()>;
}

pub(crate) fn new_io_manager(file_path: PathBuf) -> Result<Box<impl IOManager>> {
    let file_io = FileIO::new(&file_path)?;
    Ok(Box::new(file_io))
}
