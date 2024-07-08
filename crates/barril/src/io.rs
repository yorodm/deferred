use std::{path::Path, pin::Pin};

use async_fs::OpenOptions;
use bytes::{Bytes, BytesMut};
use futures_lite::{
    io::{BufReader, BufWriter},
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, Future,
};

use crate::{
    record::{Entry, Meta},
    BarrilError,
};

trait SeekReader: AsyncRead + AsyncSeek {}

impl<T> SeekReader for T where T: AsyncRead + AsyncSeek {}

pub struct DataFile {
    reader: Pin<Box<dyn SeekReader + Sync + Send>>,
    writer: Option<Pin<Box<dyn AsyncWrite + Sync + Send>>>,
    id: u32, // TODO: do we keep this here?
    offset: usize,
}


impl DataFile {
    pub fn new<P: AsRef<Path>>(
        id: u32,
        path: P,
    ) -> impl Future<Output = Result<Self, BarrilError>> {
        let path = path.as_ref().to_owned();
        async move {
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
                id,
                offset: 0,
            })
        }
    }
    pub async fn write(&mut self, key: String, data: Bytes, expires: u32) -> Result<(), BarrilError>  {
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
                self.offset = self.offset + data.len();
                w.as_mut()
                    .flush()
                    .await
                    .map_err(|e| BarrilError::IoError(e))?;
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
        let meta: Meta = meta_buffer.to_vec().try_into()?;
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
}