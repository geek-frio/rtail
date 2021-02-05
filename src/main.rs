use bytes::Bytes;
use bytes::BytesMut;
use io::Result;
use log::warn;
use memchr::memrchr_iter;
use std::future::Future;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt};
use tokio::time::{Duration, Instant};

const FLUSH_SECONDS: u64 = 1;
const FLUSH_BATCH_BYTES: usize = 4 * 1024;
const NEW_LINE_TERMINATOR: char = '\n';

type FlushRemote<T> = fn(Bytes) -> T;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut file = pop_file().await?;
    // seek to the correct position
    let meta = file.metadata().await?;
    file.seek(SeekFrom::Start(meta.len())).await?;
    // start to loop new data
    tokio::spawn(async move { loop_tail_new_log_data(file, flush_data) });
    return Ok(());
}

// Every time read one line data
//
// Batch cutting logic
// 1. Time exceed and buf has at lease one line, 1 second
// 2. Batch is bigger than 64KB we need to try to flush.
// Overceed size line
// If one line is very long, we will try to read all the one line data
async fn loop_tail_new_log_data<T: Future>(mut file: File, cbk: FlushRemote<T>) -> io::Result<()> {
    // Seek to end of file

    let mut read_bytes: usize = 0;
    let mut buf = BytesMut::with_capacity(FLUSH_BATCH_BYTES);
    let mut instant = Instant::now();

    loop {
        if read_bytes > FLUSH_BATCH_BYTES
            || (read_bytes > 0 && instant.elapsed() > Duration::from_secs(FLUSH_SECONDS))
        {
            // seek last \n from back to first
            // flush bytes from start to last '\n' idx
            // this iterator will reverse find element
            let mut iterator = memrchr_iter(NEW_LINE_TERMINATOR as u8, &buf);
            match iterator.next() {
                //  Have found no \n, maybe there is a very long line
                Some(idx) => {
                    let mut afterwards_buf = buf.split_off(idx);
                    let byts = buf.freeze();
                    println!("before length:{}, after length:{}", byts.len(), afterwards_buf.len());
                    // todo backpressure logic here
                    let _ = cbk(byts).await;
                    buf = afterwards_buf.split_off(0);

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
async fn flush_data(bytes: Bytes) -> bool {
    return true;
}

async fn pop_file() -> Result<File> {
    return Ok(File::open("test.txt").await?);
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use core::time::Duration;
    use std::future::Future;
    use std::io::SeekFrom;
    use tokio::fs::File;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncSeekExt;
    use tokio::io::AsyncWriteExt;
    use tokio::time::sleep;

    fn call_once<T: Future>(f: T) -> T::Output {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(f)
    }

    async fn read_data(bytes: Bytes) {
        if let Ok(s) = std::str::from_utf8(&bytes) {
            let s = s.trim_start();
            if s.len() > 0 {
                assert_eq!(s, "new line test data!");
            }
        }
    }

    async fn read_long_line_check(bytes: Bytes) {
        println!("read bytes is length:{}", bytes.len());
        if bytes.len() != 8191 {
            panic!("error!");
        }
    }

    #[test]
    fn test_very_long_line() {
        call_once(async {
            tokio::spawn(async {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open("./test/test_case.txt")
                    .await;
                match file {
                    Ok(mut file) => {
                        let _ = file.set_len(0).await;
                        let mut v = vec!['a' as u8; 1024 * 8];
                        v[1024 * 8 - 1] = '\n' as u8;
                        let new_line_data: &[u8] = &v;
                        let _ = file.write_all(new_line_data).await;

                        tokio::spawn(async {
                            sleep(Duration::from_secs(1)).await;
                            if let Ok(mut file) = File::open("./test/test_case.txt").await {
                                let _ = file.seek(SeekFrom::Start(0)).await;
                                let _ =
                                    super::loop_tail_new_log_data(file, read_long_line_check).await;
                            }
                        });
                    }
                    Err(e) => {
                        println!("{:?}", e)
                    }
                }
            });
            sleep(Duration::from_secs(6)).await;
        });
    }

    #[test]
    fn test_loop_write_line_data_every_second() {
        call_once(async {
            // loop write new data to
            tokio::spawn(async {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open("./test/test_case.txt")
                    .await;
                match file {
                    Ok(mut file) => {
                        let _ = file.set_len(0).await;
                        tokio::spawn(async {
                            if let Ok(mut file) = File::open("./test/test_case.txt").await {
                                let _ = file.seek(SeekFrom::Start(0)).await;
                                tokio::spawn(async {
                                    let _ = super::loop_tail_new_log_data(file, read_data).await;
                                });
                            }
                        });
                        for _ in 0..5 {
                            let new_line_data: &[u8] = b"new line test data!\n";
                            let _ = file.write_all(new_line_data).await;
                            sleep(Duration::from_millis(1500)).await;
                        }
                    }
                    Err(e) => {
                        println!("{:?}", e)
                    }
                }
            });
            sleep(Duration::from_secs(6)).await;
        });
    }
}
