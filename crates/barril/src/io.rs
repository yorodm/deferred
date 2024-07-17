use std::{path::Path, pin::Pin};

use async_fs::OpenOptions;
use bytes::{Bytes, BytesMut};
use futures_lite::{
    io::{BufReader, BufWriter},
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, Future,
};

static EXT: &str = "brl";

use crate::{
    record::{Entry, KeyMap, Meta},
    BarrilError,
};


/// Convenience trait to merge [`AsyncRead`] and [`AsyncSeek`]
trait SeekReader: AsyncRead + AsyncSeek {}

impl<T> SeekReader for T where T: AsyncRead + AsyncSeek {}

/// DataFile
/// 
/// A structure representing a data file in a `barril` directory.
pub struct DataFile {
    reader: Pin<Box<dyn SeekReader + Sync + Send>>,
    writer: Option<Pin<Box<dyn AsyncWrite + Sync + Send>>>,
    offset: usize,
    file_id: u32
}

impl DataFile {

    /// Attempt to open an existing [`DataFile`] on a given [`Path`]
    pub async fn from_path<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, BarrilError> {
        match path.as_ref().file_name() {
            Some(name) => {
                let str_name  = name.to_str()
                .ok_or(BarrilError::WrongPath(path.as_ref().to_owned()))?;
                let parts: Vec<&str>= str_name.split("_").collect();
                let id: u32 = parts[1].parse().map_err(|_| BarrilError::WrongPath(path.as_ref().to_owned()))?; // extract from file name
                DataFile::open(path, id, read_only).await
            },
            None => {
                Err(BarrilError::WrongPath(path.as_ref().to_owned()))
            },
        }
    }

    /// Attempt to create a new [`DataFile`] given it's path, name and id
    pub fn new<P: AsRef<Path>>(path: P, name: &str, id: u32) -> impl Future<Output = Result<Self, BarrilError>> {
        let file_name = format!("{}_{}.{}",name,id,EXT);
        let path = path.as_ref().to_owned().join(file_name);
        DataFile::open(path, id, false)
    }

    async fn open<P: AsRef<Path>>(path: P, id: u32, read_only: bool) -> Result<DataFile, BarrilError> {
            let writer: Option<Pin<Box<dyn AsyncWrite + Sync + Send>>> = if read_only {
                 None
            } else  {
                let w = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .await
                .map_err(|e| BarrilError::IoError(e))?;
                Some(Box::pin(BufWriter::new(w)))
            };
            let reader = OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(|e| BarrilError::IoError(e))?;
            Ok(DataFile {
                reader: Box::pin(BufReader::new(reader)),
                writer,
                offset: 0,
                file_id: id
            })
    }

    /// Create a new entry represented by `key` and `data`
    pub async fn write(
        &mut self,
        key: String,
        data: Bytes,
        expires: u32,
    ) -> Result<(), BarrilError> {
        let entry = Entry::new(key, data, expires);
        self.write_entry(entry).await
    }

    async fn write_entry(&mut self, entry: Entry) -> Result<(), BarrilError> {
        let data: Bytes = entry.into();
        match &mut self.writer {
            Some(w) => {
                w.as_mut()
                    .write_all(&data)
                    .await
                    .map_err(|e| BarrilError::IoError(e))?;
                w.as_mut()
                    .flush()
                    .await
                    .map_err(|e| BarrilError::IoError(e))?;
                self.offset = self.offset + data.len(); // Only move the offset if we succeeded w
            }
            None => {
                return Err(BarrilError::NoActiveData);
            }
        }
        Ok(())
    }

    /// Read a new entry from `pos`
    pub async fn read(&mut self, pos: usize) -> Result<Entry, BarrilError> {
        self.reader
            .as_mut()
            .seek(std::io::SeekFrom::Start(pos as u64))
            .await
            .map_err(|e| BarrilError::IoError(e))?;
        let mut meta_buffer = BytesMut::new();
        self.reader
            .as_mut()
            .read_exact(&mut meta_buffer)
            .await
            .map_err(|e| BarrilError::IoError(e))?;
        let meta: Meta = meta_buffer.freeze().try_into()?;
        let mut key_buffer = BytesMut::new();
        self.reader
            .as_mut()
            .read_exact(&mut key_buffer)
            .await
            .map_err(|e| BarrilError::IoError(e))?;
        let mut data_buffer = BytesMut::new();
        self.reader
            .as_mut()
            .read_exact(&mut data_buffer)
            .await
            .map_err(|e| BarrilError::IoError(e))?;
        Ok(Entry {
            meta,
            key: String::from_utf8_lossy(&key_buffer).into_owned(),
            data: data_buffer.freeze(),
        })
    }
    
    /// Close the writer side of this [`DataFile`]
    pub async fn close(&mut self) -> Result<(), BarrilError> {
        match &mut self.writer.take() {
            Some(w) => w
                .as_mut()
                .close()
                .await
                .map_err(|e| BarrilError::IoError(e)),
            None => Ok(()),
        }
    }
}

/// Save a hints file at the given [`Path`]
pub(crate) async fn save_hints<P: AsRef<Path>>(
    key_map: &KeyMap,
    path: P,
) -> Result<(), BarrilError> {
    todo!()
}

/// Load a hints file from the given [`Path`]
pub(crate) async fn load_hints<P: AsRef<Path>>(
    key_map: &KeyMap,
    path: P,
) -> Result<KeyMap, BarrilError> {
    todo!()
}
