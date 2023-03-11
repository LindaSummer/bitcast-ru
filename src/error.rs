use std::{io::Error, result};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("failed to read from file")]
    FailToReadFromDataFile(#[from] Error),

    #[error("failed to write to file")]
    FailToWriteToDataFile(),
}

pub type Result<T> = result::Result<T, Errors>;
