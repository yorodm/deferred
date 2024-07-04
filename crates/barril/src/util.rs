use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarrilError{
    #[error("Malformed data")]
    MalformedData,
    #[error("I/O Error")]
    IoError(#[from] io::Error)
}