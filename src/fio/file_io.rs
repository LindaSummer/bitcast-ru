use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::PathBuf,
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

impl FileIO {
    pub fn new(file_path: &PathBuf) -> Result<Self> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_path.as_path())
            .map(|f| FileIO {
                fd: Arc::new(RwLock::new(f)),
            })
            .map_err(|e| {
                error!("failed to open file: {:?}, error: {:?}", file_path, e);
                Errors::FailToOpenDataFile(e.to_string())
            })
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        read_guard.read_at(buf, offset).map_err(|e| {
            error!("read data file failed: {:?}", e);
            Errors::FailToReadFromDataFile(e.to_string())
        })
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        write_guard.write(buf).map_err(|e| {
            error!("write data file failed: {:?}", e);
            Errors::FailToWriteToDataFile(e.to_string())
        })
    }

    fn sync(&mut self) -> Result<()> {
        let read_guard = self.fd.read();
        read_guard.sync_all().map_err(|e| {
            error!("sync data file failed: {:?}", e);
            Errors::FailToReadFromDataFile(e.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, env::temp_dir, fs, str::FromStr};

    use uuid::Uuid;

    use super::*;

    fn temp_file_path() -> String {
        temp_dir()
            // .join("bitcast-rs-test")
            .join(Uuid::new_v4().to_string())
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from_str(temp_file_path().as_str());
        assert!(path.is_ok());
        let path = path.unwrap();

        let file = FileIO::new(path.borrow());
        assert!(file.is_ok());

        let mut file = file.unwrap();

        assert_eq!(file.write(&[1, 2, 3]), Ok(3));
        assert_eq!(file.write("sadads".as_bytes()), Ok(6));
        assert_eq!(file.write(&[]), Ok(0));
        assert_eq!(file.write(&[1, 2, 3, 34, 88]), Ok(5));
        assert_eq!(file.write(&[1, 2, 3, 4, 5, 1, 8, 8, 9]), Ok(9));
        assert_eq!(file.write(&[1, 2, 3]), Ok(3));

        assert!(fs::remove_file(path).is_ok());
    }

    #[test]
    fn test_file_read() {
        let path = PathBuf::from_str(temp_file_path().as_str());
        assert!(path.is_ok());
        let path = path.unwrap();

        let file = FileIO::new(path.borrow());
        assert!(file.is_ok());

        let mut file = file.unwrap();
        assert_eq!(file.write(&[1, 2, 3]), Ok(3));

        let mut buf = [0u8; 1];
        assert_eq!(file.read(&mut buf, 0), Ok(1));
        assert_eq!(buf, [1]);

        let mut buf = [0u8; 2];
        assert_eq!(file.read(&mut buf, 0), Ok(2));
        assert_eq!(buf, [1, 2]);

        let mut buf = [0u8; 3];
        assert_eq!(file.read(&mut buf, 0), Ok(3));
        assert_eq!(buf, [1, 2, 3]);

        let mut buf = [0u8; 4];
        assert_eq!(file.read(&mut buf, 0), Ok(3));
        assert_eq!(buf, [1, 2, 3, 0]);

        assert!(fs::remove_file(path).is_ok());
    }

    #[test]
    fn test_file_sync() {
        let path = PathBuf::from_str(temp_file_path().as_str());
        assert!(path.is_ok());
        let path = path.unwrap();

        let file = FileIO::new(path.borrow());
        assert!(file.is_ok());

        let mut file = file.unwrap();

        assert_eq!(file.write(&[1, 2, 3]), Ok(3));
        assert_eq!(file.write("sadads".as_bytes()), Ok(6));
        assert_eq!(file.write(&[]), Ok(0));
        assert_eq!(file.write(&[1, 2, 3, 34, 88]), Ok(5));
        assert_eq!(file.write(&[1, 2, 3, 4, 5, 1, 8, 8, 9]), Ok(9));
        assert_eq!(file.write(&[1, 2, 3]), Ok(3));

        assert_eq!(file.sync(), Ok(()));

        assert!(fs::remove_file(path).is_ok());
    }
}
