/// IOManager provide a abstract interface for io manuplation
pub trait IOManager {
    /// read from @offset of a file
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Option<u32>;
}
