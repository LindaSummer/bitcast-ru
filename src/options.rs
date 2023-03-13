use std::path::PathBuf;

pub struct Options {
    /// database path of directory
    pub dir_path: PathBuf,
    /// active datafile size threshold
    pub datafile_size: u64,
    /// database file directory
    pub file_dir: String,
}
