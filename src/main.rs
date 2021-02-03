use bytes::Bytes;
use bytes::BytesMut;
use io::Result;
use log::warn;
use memchr::memrchr_iter;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt};
use tokio::time::{Duration, Instant};
use std::future::Future;

const FLUSH_SECONDS: u64 = 1;
const FLUSH_BATCH_BYTES: usize = 64 * 1024;
const NEW_LINE_TERMINATOR: char = '\n';

type FlushRemote<T> =  fn(Bytes) -> T;

#[tokio::main]
async fn main() -> io::Result<()> {
    let file = pop_file().await?;
    tokio::spawn(async move { loop_tail_new_log_data(file, flush_data)});
    return Ok(());
}

async fn loop_tail_new_log_data<T: Future>(mut file: File, cbk: FlushRemote<T>) -> io::Result<()> {
    // Seek to end of file
    let meta = file.metadata().await?;

    // Every time read one line data
    //
    // Batch cutting logic
    // 1. Time exceed and buf has at lease one line, 1 second
    // 2. Batch is bigger than 64KB and there is several line data.
    //
    // Overceed size line
    // If one line is very long, we will cut off the line, skip to another line
    //
    // Buffer channel
    // for backpressure
    let mut read_bytes: usize = 0;
    let mut buf = BytesMut::with_capacity(FLUSH_BATCH_BYTES);
    let mut instant = Instant::now();

    file.seek(SeekFrom::Start(meta.len())).await?;
    loop {
        if read_bytes > FLUSH_BATCH_BYTES || instant.elapsed() > Duration::from_secs(FLUSH_SECONDS)
        {
            // seek last \n from back to first
            // flush bytes from start to last '\n' idx
            // this iterator will reverse find element
            let mut iterator = memrchr_iter(NEW_LINE_TERMINATOR as u8, &buf);
            match iterator.next() {
                //  Have found no \n, maybe there is a very long line
                Some(idx) => {
                    let afterwards_buf = buf.split_off(idx);
                    let byts = buf.freeze();
                    // todo backpressure logic here
                    let _ = cbk(byts).await;
                    buf = afterwards_buf;

                    instant = Instant::now();
                    read_bytes = 0;
                }
                None => {
                    // continue to read, every time in the loop we check if we can find a new \n
                    warn!("we have met a very long line, continue to find \n ");
                }
            }
        }
        let size = file.read_buf(&mut buf).await?;
        read_bytes += size;
    }
}

// todo
// usually we use this method to send to remote peer
async fn flush_data(bytes: Bytes) {
    println!("start to flush data to remote peer, data is {:?}", bytes);
}

async fn pop_file() -> Result<File> {
    return Ok(File::open("test.txt").await?);
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use std::future::Future;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use tokio::time::sleep;
    use core::time::Duration;

    fn call_once<T: Future>(f: T) -> T::Output {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(f)
    }

    #[test]
    fn test_loop_write_line_data_every_second() {
        call_once(async {
            // loop write new data to 
            tokio::spawn(async {
                if let Ok(mut file) = File::open("./test/test_case.txt").await {
                    for _ in 0..10 {
                        let _ = file.write_all(b"new line test data!").await;
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            });
        });
    }
}
