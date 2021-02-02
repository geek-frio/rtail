use bytes::buf::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use io::Result;
use log::{info, trace, warn};
use memchr::memchr;
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::{borrow::BorrowMut, io::SeekFrom};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::io::{self, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

const FLUSH_MILI_SECONDS: u8 = 1;
const FLUSH_BATCH_BYTES: u32 = 64 * 1024;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut file = pop_file().await?;
    let delimeter: u8 = b'\n';

    // Seek to end of file
    let meta = file.metadata().await?;
    file.seek(SeekFrom::Start(meta.len())).await?;

    // Every time read one line data
    //
    // Batch cutting logic
    // 1. Time exceed and buf has at lease one line, 1 second
    // 2. Batch is bigger than 64KB, flush.
    //
    // Overceed size line
    // If one line's size overceed 32kb, force cutting off this line
    //
    // Buffer channel
    // for backpressure
    let mut read_bytes = 0;
    let mut buf = BytesMut::with_capacity(1024 * 64);

    loop {
        if read_bytes > 32 * 1024 {
            // seek last \n
            // flush bytes from start to last \n
            // if there is no
            flush_data(&buf).await;
        }
        let size = file.read_buf(&mut buf).await?;
        read_bytes += size;
    }
    return Ok(());
}

async fn flush_data(bytes: &BytesMut) {}

async fn pop_file() -> Result<File> {
    return Ok(File::open("test.txt").await?);
}

mod tests {
    #[cfg(test)]
    use bytes::BytesMut;
    use std::future::Future;
    #[cfg(test)]
    use std::io::SeekFrom;
    #[cfg(test)]
    use tokio::fs::File;
    #[cfg(test)]
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn call_once<T: Future>(f: T) -> T::Output {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(f)
    }

    #[test]
    fn test_bytes_mut_multiple_read_buf() {
        call_once(async {
            if let Ok(mut file) = File::open("./test_case.txt").await {
                if let Ok(meta) = file.metadata().await {
                    let mut buf = BytesMut::with_capacity(10);
                    for _ in 0..10 {
                        let size = file.read_buf(&mut buf).await;
                        println!("{:?}, buf:{:?}", size, buf);
                    }
                }
            }
        })
    }
}
