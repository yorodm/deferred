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


trait SeekReader: AsyncRead + AsyncSeek {}

impl<T> SeekReader for T where T: AsyncRead + AsyncSeek {}

pub struct DataFile {
    reader: Pin<Box<dyn SeekReader + Sync + Send>>,
    writer: Option<Pin<Box<dyn AsyncWrite + Sync + Send>>>,
    offset: usize,
    file_id: u32
}

impl DataFile {

    pub async fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, BarrilError> {
        match path.as_ref().file_name() {
            Some(name) => {
                let str_name  = name.to_str()
                .ok_or(BarrilError::WrongPath(path.as_ref().to_owned()))?;
                let parts: Vec<&str>= str_name.split("_").collect();
                let id: u32 = parts[1].parse().map_err(|_| BarrilError::WrongPath(path.as_ref().to_owned()))?; // extract from file name
                DataFile::open(path, id).await
            },
            None => {
                Err(BarrilError::WrongPath(path.as_ref().to_owned()))
            },
        }
    }

    pub fn new<P: AsRef<Path>>(path: P, name: &str, id: u32) -> impl Future<Output = Result<Self, BarrilError>> {
        let file_name = format!("{}_{}.{}",name,id,EXT);
        let path = path.as_ref().to_owned().join(file_name);
        DataFile::open(path, id)
    }

    async fn open<P: AsRef<Path>>(path: P, id: u32) -> Result<DataFile, BarrilError> {
            let writer = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .await
                .map_err(|e| BarrilError::IoError(e))?;
            let reader = OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(|e| BarrilError::IoError(e))?;
            Ok(DataFile {
                reader: Box::pin(BufReader::new(reader)),
                writer: Some(Box::pin(BufWriter::new(writer))),
                offset: 0,
                file_id: id
            })
    }

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

pub(crate) async fn save_hints<P: AsRef<Path>>(
    key_map: &KeyMap,
    path: P,
) -> Result<(), BarrilError> {
    todo!()
}

pub(crate) async fn load_hints<P: AsRef<Path>>(
    key_map: &KeyMap,
    path: P,
) -> Result<KeyMap, BarrilError> {
    todo!()
}
