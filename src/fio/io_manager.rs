use crate::error::Result;

/// IOManager provide a abstract interface for io manuplation
pub trait IOManager {
    /// read from @offset of a file
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// write buffer to a file
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// flush data to consistant file
    fn sync(&mut self) -> Result<()>;
}
