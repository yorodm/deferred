use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarrilError {
    #[error("Data error")]
    DataError,
    #[error("I/O Error")]
    IoError(#[from] io::Error),
    #[error("This data file is not active and cannot be used for writing")]
    NoActiveData,
}

pub (crate) fn timestamp() -> i64 {
    chrono::Local::now().timestamp_nanos_opt().expect("Could not get timestamp!")
}
