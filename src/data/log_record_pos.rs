/// LogRecordPos description of a record position with file id and offset
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
