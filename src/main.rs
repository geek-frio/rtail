use async_trait::async_trait;
use bytes::BytesMut;
use io::Result;
use memchr::memrchr;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt};
use tokio::time::{Duration, Instant};

const FLUSH_SECONDS: u64 = 1;
const FLUSH_BATCH_BYTES: usize = 4 * 1024;
const NEW_LINE_TERMINATOR: char = '\n';

#[async_trait]
trait ConsumeBytes {
    async fn consume(&self, data: (&[u8], &[u8]));
}

struct FlushRemote;

#[async_trait]
impl ConsumeBytes for FlushRemote {
    // Flush data to remote peer
    async fn consume(&self, _: (&[u8], &[u8])) {}
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut file = pop_file().await?;
    // seek to the correct position
    let meta = file.metadata().await?;
    file.seek(SeekFrom::Start(meta.len())).await?;

    // start to loop new data
    tokio::spawn(async move {
        let consume_byts = FlushRemote {};
        let _ = loop_tail_new_log_data(file, consume_byts).await;
    });
    return Ok(());
}

async fn loop_tail_new_log_data<T>(mut file: File, consume: T) -> io::Result<()>
where
    T: ConsumeBytes,
{
    // Seek to end of file
    let mut read_bytes: usize = 0;
    // We use two buckets to temp buffer data
    let mut remain = BytesMut::with_capacity(FLUSH_BATCH_BYTES);
    let mut current = BytesMut::with_capacity(FLUSH_BATCH_BYTES);
    // Every FLUSH_SECONDS flush data to remote
    let mut instant = Instant::now();

    loop {
        if read_bytes > FLUSH_BATCH_BYTES
            || (read_bytes > 0 && instant.elapsed() > Duration::from_secs(FLUSH_SECONDS))
        {
            // Seek last \n from back to first // flush bytes from start to last '\n' idx
            if let Some(idx) = memrchr(NEW_LINE_TERMINATOR as u8, &current) {
                let line_byts = current.split_to(idx);
                // Flush data to remote peer
                let l = &remain.as_ref()[0..remain.len()];
                let r = &line_byts.as_ref()[0..line_byts.len()];
                consume.consume((l, r)).await;
                // Swap remain and current
                remain.clear();
                // Remain bytes which is behind '\n' will be stored in remain
                let tmp = remain;
                remain = current;
                current = tmp;
                // Status reset
                read_bytes = 0;
                instant = Instant::now();
            }
        }
        let size = file.read_buf(&mut current).await?;
        read_bytes += size;
    }
}

async fn pop_file() -> Result<File> {
    return Ok(File::open("test.txt").await?);
}

#[cfg(test)]
mod tests {
    use super::ConsumeBytes;
    use async_trait::async_trait;
    use core::time::Duration;
    use rand::prelude::*;
    use std::future::Future;
    use std::io::SeekFrom;
    use tokio::fs::File;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncSeekExt;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::sleep;

    fn call_once<T: Future>(f: T) -> T::Output {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(f)
    }

    // unsafe use just for test
    // async fn normal_read_check(ctx: Arc<Box<Context<Receiver<String>>>>, bytes: Bytes) {
    //     unsafe {
    //         let r = (ctx.t.borrow() as *const Receiver<String> as *mut Receiver<String>)
    //             .as_mut()
    //             .unwrap();
    //         let m = memchr_iter('\n' as u8, &bytes);
    //         let mut start_idx = 0;
    //         for idx in m.into_iter() {
    //             if idx == 0 {
    //                 println!("skip");
    //                 continue;
    //             }
    //             let as_slice = bytes.as_ref();
    //             let ary = &as_slice[start_idx..idx];
    //             start_idx = idx;
    //             assert_eq!(std::str::from_utf8(ary).unwrap(), r.recv().await.unwrap());
    //         }
    //     };
    // }

    struct NormalTest {
        rx: Receiver<String>,
    }

    #[async_trait]
    impl ConsumeBytes for NormalTest {
        async fn consume(&self, data: (&[u8], &[u8])) {
            println!(
                "{}{}",
                std::str::from_utf8(data.0).unwrap(),
                std::str::from_utf8(data.1).unwrap()
            );
            println!("\n\n\n=============\n");
        }
    }

    #[test]
    fn test_normal_read() {
        call_once(async {
            let (tx, rx) = mpsc::channel::<String>(100);
            tokio::spawn(async move {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open("./test/test_case.txt")
                    .await;
                match file {
                    Ok(mut file) => {
                        let _ = file.set_len(0).await;
                        for _ in 0..1000 {
                            let mut s = String::new();
                            for _ in 0..10000 {
                                s.push('a');
                            }
                            s.push('E');
                            s.push('\n');
                            let _ = file.write(s.as_bytes()).await;
                            let _ = tx.send(s).await;
                        }
                        let _ = tx.send("QUIT".to_string()).await;
                    }
                    Err(e) => {
                        println!("{:?}", e)
                    }
                }
            });
            tokio::spawn(async move {
                sleep(Duration::from_secs(1)).await;
                let normal_test = NormalTest { rx };
                if let Ok(mut file) = File::open("./test/test_case.txt").await {
                    let _ = file.seek(SeekFrom::Start(0)).await;
                    let _ = super::loop_tail_new_log_data(file, normal_test).await;
                }
            });
            sleep(Duration::from_secs(6)).await;
        });
    }

    //     #[test]
    //     fn test_very_long_line() {
    //         call_once(async {
    //             let file = OpenOptions::new()
    //                 .read(true)
    //                 .write(true)
    //                 .open("./test/test_case.txt")
    //                 .await;
    //             match file {
    //                 Ok(mut file) => {
    //                     let _ = file.set_len(0).await;
    //                     let mut v = vec!['a' as u8; 1024 * 8];
    //                     v[1024 * 8 - 1] = '\n' as u8;
    //                     let new_line_data: &[u8] = &v;
    //                     let _ = file.write_all(new_line_data).await;

    //                     tokio::spawn(async {
    //                         sleep(Duration::from_secs(1)).await;
    //                         if let Ok(mut file) = File::open("./test/test_case.txt").await {
    //                             let _ = file.seek(SeekFrom::Start(0)).await;
    //                             let _ = super::loop_tail_new_log_data(file, read_long_line_check).await;
    //                         }
    //                     });
    //                 }
    //                 Err(e) => {
    //                     println!("{:?}", e)
    //                 }
    //             }
    //             sleep(Duration::from_secs(6)).await;
    //         });
    //     }

    //     #[test]
    //     fn test_loop_write_line_data_every_second() {
    //         call_once(async {
    //             // loop write new data to
    //             tokio::spawn(async {
    //                 let file = OpenOptions::new()
    //                     .read(true)
    //                     .write(true)
    //                     .open("./test/test_case.txt")
    //                     .await;
    //                 match file {
    //                     Ok(mut file) => {
    //                         let _ = file.set_len(0).await;
    //                         tokio::spawn(async {
    //                             if let Ok(mut file) = File::open("./test/test_case.txt").await {
    //                                 let _ = file.seek(SeekFrom::Start(0)).await;
    //                                 async fn read_data(bytes: Bytes) {
    //                                     if let Ok(s) = std::str::from_utf8(&bytes) {
    //                                         let s = s.trim_start();
    //                                         if s.len() > 0 {
    //                                             assert_eq!(s, "new line test data!");
    //                                         }
    //                                     }
    //                                 }
    //                                 tokio::spawn(async {
    //                                     let _ = super::loop_tail_new_log_data(file, read_data).await;
    //                                 });
    //                             }
    //                         });
    //                         for _ in 0..5 {
    //                             let new_line_data: &[u8] = b"new line test data!\n";
    //                             let _ = file.write_all(new_line_data).await;
    //                             sleep(Duration::from_millis(1500)).await;
    //                         }
    //                     }
    //                     Err(e) => {
    //                         println!("{:?}", e)
    //                     }
    //                 }
    //             });
    //             sleep(Duration::from_secs(6)).await;
    //         });
    // }
}
