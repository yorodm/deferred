//! Barril
//! 
//! `barril` is a simple implementation of [bitcask](https://github.com/basho/bitcask) using
//! `async` to perform I/O operations
mod engine;
mod io;
mod record;
mod util;
pub use engine::Engine;
pub use io::DataFile;
pub use util::BarrilError;
