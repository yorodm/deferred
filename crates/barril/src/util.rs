use std::{io, path::PathBuf};

use thiserror::Error;
/// Different error types returned by `barril`
#[derive(Error, Debug)]
pub enum BarrilError {
    /// Signals data format errors
    #[error("Data error")]
    DataError,
    /// Signals that an I/O error has occurred
    #[error("I/O Error")]
    IoError(#[from] io::Error),
    /// Signals that the engine is trying to use a [`crate::DataFile`] that's not open for writing
    #[error("This data file is not active and cannot be used for writing")]
    NoActiveData,
    /// Signals that the path given doesn't have a valid file name
    #[error("Error processing file: {0}")]
    WrongPath(PathBuf),
}

pub(crate) fn timestamp() -> i64 {
    chrono::Local::now()
        .timestamp_nanos_opt()
        .expect("Could not get timestamp!")
}
