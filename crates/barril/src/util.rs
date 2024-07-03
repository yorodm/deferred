use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarrilError{
    #[error("Malformed data")]
    MalformedData,
}