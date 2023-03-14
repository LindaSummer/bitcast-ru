use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
    #[error("failed to read from file")]
    FailToReadFromDataFile(String),

    #[error("failed to sync file")]
    FailToSyncDataFile(String),

    #[error("failed to write to file")]
    FailToWriteToDataFile(String),

    #[error("failed to open file")]
    FailToOpenDataFile(String),

    #[error("failed to close file")]
    FailToCloseDataFile(String),

    #[error("key is empty")]
    EmptyKey,

    #[error("update memory index failed")]
    FailToUpdateIndex,

    #[error("no such key found")]
    KeyNotFound,

    #[error("datafile in index does not exist")]
    DataFileNotFound,

    #[error("database directory is empty")]
    InvalidDatabasePath,

    #[error("datafile size must greater than zero")]
    DatafileSizeTooSmall,

    #[error("create database directory failed")]
    FailToCreateDatabaseDirectory,

    #[error("read database directory failed")]
    FailToReadDatabaseDirectory,

    #[error("database directory may be corrupted")]
    DatabaseFileCorrupted,
}

pub type Result<T> = result::Result<T, Errors>;
