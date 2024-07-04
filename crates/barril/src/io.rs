use std::path::Path;

use async_fs::OpenOptions;
use futures_lite::{io::{BufReader, BufWriter}, AsyncRead, AsyncWrite, AsyncWriteExt, Future};

use crate::{record::Entry, BarrilError};


pub struct DataFile<R: AsyncRead, W: AsyncWrite>  {
    reader: BufReader<R>,
    writer: BufWriter<W>,
    id: u32,
    offset: usize
}


impl<R: AsyncRead,W: AsyncWrite> DataFile<R,W>  {
    fn new<P: AsRef<Path>>(id: u32, path: P) -> impl Future<Output = Result<Self, BarrilError>>{
        let path = path.as_ref().to_owned();
        async move {
            let reader = OpenOptions::new().read(true).open(&path).await.map_err(|e| BarrilError::IoError(e))?;
            let writer  = OpenOptions::new().append(true).open(&path).await.map_err(|e| BarrilError::IoError(e))?;
            Ok(DataFile{reader: BufReader::new(reader), writer: BufWriter::new(writer), id, offset: 0})
        }
    }

    async fn write(&mut self, entry: Entry) -> Result<(), BarrilError> {
        let data: Vec<u8> = entry.into();
        self.writer.write_all(&data).await.map_err(|e| BarrilError::IoError(e))?;
        Ok(())
    }
}