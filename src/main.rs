#[macro_use]
extern crate lazy_static;
extern crate notify;

mod watch;

use async_trait::async_trait;
use bytes::BytesMut;
use memchr::memrchr;
use std::collections::HashMap;
use std::io;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Duration, Instant};
use tokio::{
    fs::File,
    sync::oneshot::{channel, Sender},
};
use watch::FileEvent;
use watch::LogFilesWatcher;
use watch::TailPosition;

lazy_static! {
    pub static ref STOP_CTRL: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

const FLUSH_SECONDS: u64 = 1;
const FLUSH_BATCH_BYTES: usize = 4 * 1024;
const NEW_LINE_TERMINATOR: char = '\n';

#[async_trait]
trait ConsumeBytes {
    async fn consume(&mut self, data: (&[u8], &[u8]));
}

struct FlushRemote;

#[async_trait]
impl ConsumeBytes for FlushRemote {
    // Flush data to remote peer
    async fn consume(&mut self, _: (&[u8], &[u8])) {}
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let (log_file_watcher, receiver) = LogFilesWatcher::init(vec![], vec![]);

    // Start scan task
    // mock dir patterns, file_patterns
    std::thread::spawn(move || {
        log_file_watcher.root_start(vec!["./".to_string()]);
    });
    // start to loop new data
    let watch_res = watch_process_new_file(receiver).await;
    if let Err(e) = watch_res {
    println!("");
    return Ok(());
}

async fn seek_tail(path: PathBuf, pos: TailPosition) -> Result<(), io::Error> {
    let mut file = File::open(path.as_path()).await?;
    if pos == TailPosition::End {
        file.seek(SeekFrom::End(0)).await?;
    }
    let consume_byts = FlushRemote {};
    loop_tail_new_log_data(file, consume_byts).await?;
    Ok(())
}

enum TailOperation {
    Stop,
}

async fn process_file_event(
    file_event: FileEvent,
    watching_files: &mut HashMap<PathBuf, Sender<TailOperation>>,
) -> Result<(), std::io::Error> {
    match file_event.event_type {
        watch::FileEventType::Create => {
            if watching_files.contains_key(&file_event.path) {
                return Ok(());
            }
            let (send, recv) = channel::<TailOperation>();
            watching_files.insert(file_event.path.clone(), send);
            seek_tail(file_event.path.clone(), file_event.position).await?;
            let flush_remote = FlushRemote {};
            let file = File::open(file_event.path).await?;
            tokio::spawn(loop_tail_new_log_data(file, flush_remote));
        }
        watch::FileEventType::Delete => {}
    }
    Ok(())
}

async fn watch_process_new_file(mut receiver: UnboundedReceiver<FileEvent>) -> Result<(), io::Error> {
    let mut watching_files: HashMap<PathBuf, Sender<TailOperation>> = HashMap::new();
    loop {
        let file_event = receiver.recv().await;
        if let Some(file_event) = file_event {
            process_file_event(file_event, &mut watching_files).await?;
        }
    }
}

async fn loop_tail_new_log_data<T>(mut file: File, mut consume: T) -> io::Result<()>
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
        if STOP_CTRL.load(Ordering::Relaxed) {
            break;
        }
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ConsumeBytes;
    use async_trait::async_trait;
    use std::io::SeekFrom;
    use std::str::from_utf8;
    use std::time::Duration;
    use std::{future::Future, sync::atomic::Ordering};
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

    struct NormalTest {
        rx: Receiver<String>,
    }

    #[async_trait]
    impl ConsumeBytes for NormalTest {
        async fn consume(&mut self, data: (&[u8], &[u8])) {
            let s = format!(
                "{}{}",
                from_utf8(data.0).unwrap(),
                from_utf8(data.1).unwrap()
            );
            let iter = s.split('\n');
            for x in iter {
                if x.len() == 0 {
                    continue;
                }
                let sent_s = self.rx.recv().await;
                assert_eq!(sent_s.unwrap(), x);
            }
        }
    }

    struct BlankTest;

    #[async_trait]
    impl ConsumeBytes for BlankTest {
        async fn consume(&mut self, _: (&[u8], &[u8])) {}
    }

    #[test]
    fn test_delete_when_in_read() {
        const FILE_NAME: &str = "./test/test_case.txt";
        call_once(async {
            let normal_test = BlankTest {};
            std::thread::spawn(|| {
                // wait for a while and then delete the file
                std::thread::sleep(std::time::Duration::from_millis(500));
                let remove_res = std::fs::remove_file(FILE_NAME);
                println!("remove result is {:?}", remove_res);
            });
            if let Ok(mut file) = File::open(FILE_NAME).await {
                let _ = file.seek(SeekFrom::Start(0)).await;
                let res = super::loop_tail_new_log_data(file, normal_test).await;
                println!("loop res is {:?}", res);
            }
        });
    }

    #[test]
    fn test_normal_read() {
        call_once(async {
            let (tx, rx) = mpsc::channel::<String>(100);
            let (signal_tx, mut signal_rx) = mpsc::channel::<()>(2);
            let s1 = signal_tx.clone();
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
                            let _ = tx.send(s.trim_end().to_string()).await;
                        }
                        let _ = tx.send("QUIT".to_string()).await;
                    }
                    Err(e) => {
                        println!("{:?}", e)
                    }
                }
                let _ = s1.send(()).await;
            });
            let s2 = signal_tx.clone();
            tokio::spawn(async move {
                let normal_test = NormalTest { rx };
                if let Ok(mut file) = File::open("./test/test_case.txt").await {
                    let _ = file.seek(SeekFrom::Start(0)).await;
                    let _ = super::loop_tail_new_log_data(file, normal_test).await;
                }
                let _ = s2.send(()).await;
            });
            // wait to complete
            signal_rx.recv().await;
            super::STOP_CTRL.store(true, std::sync::atomic::Ordering::Relaxed);
            // wait for data to be read
            sleep(Duration::from_millis(300)).await;
            signal_rx.recv().await;
        });
    }
    struct LongLineTest;

    #[async_trait]
    impl ConsumeBytes for LongLineTest {
        async fn consume(&mut self, data: (&[u8], &[u8])) {
            assert_eq!(data.0.len() + data.1.len(), 1024 * 8 - 1);
            // has read the data
            super::STOP_CTRL.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn test_very_long_line() {
        call_once(async {
            let (signal_tx, mut signal_rx) = mpsc::channel::<()>(2);
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
                    tokio::spawn(async move {
                        if let Ok(mut file) = File::open("./test/test_case.txt").await {
                            let _ = file.seek(SeekFrom::Start(0)).await;
                            let consumer = LongLineTest {};
                            let _ = super::loop_tail_new_log_data(file, consumer).await;
                            let _ = signal_tx.send(()).await;
                        }
                    });
                }
                Err(e) => {
                    println!("{:?}", e)
                }
            }
            signal_rx.recv().await;
        });
    }

    struct LoopWriteLineDataTest {
        times: usize,
    }

    #[async_trait]
    impl ConsumeBytes for LoopWriteLineDataTest {
        async fn consume(&mut self, data: (&[u8], &[u8])) {
            let a = String::from("abc");
            println!("af:{}", a);

            let s = format!(
                "{}{}",
                std::str::from_utf8(data.0).unwrap_or(""),
                std::str::from_utf8(data.1).unwrap()
            );
            let iter = s.split('\n');
            for m in iter {
                if m.len() > 0 {
                    self.times += 1;
                }
            }
            if self.times == 1000 {
                println!("Times is already 1000");
                super::STOP_CTRL.store(true, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn test_loop_write_line_data_periodicaly() {
        call_once(async {
            let (signal_tx, mut signal_rx) = mpsc::channel::<()>(1);
            // loop write new data to
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open("./test/test_case.txt")
                .await;
            match file {
                Ok(mut file) => {
                    let _ = file.set_len(0).await;
                    if let Ok(mut file) = File::open("./test/test_case.txt").await {
                        let _ = file.seek(SeekFrom::Start(0)).await;
                        tokio::spawn(async move {
                            let _ = super::loop_tail_new_log_data(
                                file,
                                LoopWriteLineDataTest { times: 0 },
                            )
                            .await;
                            let _ = signal_tx.send(()).await;
                        });
                    }
                    for _ in 0..1000 {
                        let new_line_data: &[u8] = b"new line test data!\n";
                        let _ = file.write_all(new_line_data).await;
                        sleep(Duration::from_millis(5)).await;
                    }
                }
                Err(e) => {
                    println!("{:?}", e)
                }
            };
            signal_rx.recv().await;
        });
    }
}
