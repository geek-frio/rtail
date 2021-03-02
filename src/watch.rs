use notify::DebouncedEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
use std::collections::HashSet;
use std::fs;
use std::fs::canonicalize;
use std::io;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use walkdir::WalkDir;

macro_rules! regex_pattern {
    ($name:expr) => {
        $name
            .iter()
            .map(|a| Regex::new(a))
            .filter(|a| a.is_ok())
            .map(|a| a.unwrap())
            .collect()
    };
}

#[derive(PartialEq, Eq, Debug)]
pub enum TailPosition {
    Start,
    End,
}

pub struct LogFilesWatcher {
    trace_files: Arc<Mutex<HashMap<PathBuf, TailPosition>>>,
    tx: UnboundedSender<(PathBuf, TailPosition)>,
    dir_patterns: Vec<Regex>,
    file_patterns: Vec<Regex>,
    watching_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

impl LogFilesWatcher {
    pub fn init(
        dir_patterns: Vec<String>,
        file_patterns: Vec<String>,
    ) -> (LogFilesWatcher, UnboundedReceiver<(PathBuf, TailPosition)>) {
        let dir_patterns = regex_pattern!(dir_patterns);
        let file_patterns = regex_pattern!(file_patterns);
        let (tx, rx) = mpsc::unbounded_channel::<(PathBuf, TailPosition)>();
        (
            LogFilesWatcher {
                trace_files: Arc::new(Mutex::new(HashMap::new())),
                tx,
                dir_patterns,
                file_patterns,
                watching_dirs: Arc::new(Mutex::new(HashSet::new())),
            },
            rx,
        )
    }

    pub fn spawn_watcher_scan(&self, watch_dir: PathBuf) -> Result<(), io::Error> {
        let (notify, waiter) = channel();
        self.spawn_watcher(watch_dir.clone(), notify)?;
        self.scan_files(watch_dir.clone(), waiter)?;
        let watching_dirs = self.watching_dirs.lock().into_iter().next();
        if let Some(mut w) = watching_dirs {
            w.insert(watch_dir);
        }
        return Ok(());
    }

    pub fn spawn_watcher(&self, watch_dir: PathBuf, notify: Sender<bool>) -> Result<(), io::Error> {
        let path_tx = self.tx.clone();
        thread::spawn(move || {
            let (tx, rx) = channel();
            let watch_res =
                Watcher::new(tx, Duration::from_secs(2)).map(|mut watcher: RecommendedWatcher| {
                    watch_dir.to_str().map(|s| {
                        if let Err(_) = watcher.watch(s, RecursiveMode::NonRecursive) {
                            println!("watch dir:{:?} err", watch_dir);
                        }
                        let _ = notify.send(true);
                        rx.iter().for_each(|event| {
                            if let DebouncedEvent::Create(path) = event {
                                if let Ok(path) = canonicalize(path) {
                                    let _ = path_tx.send((path, TailPosition::Start));
                                }
                            }
                        })
                    });
                });
            if watch_res.is_err() {
                let _ = notify.send(false);
            }
        });
        Ok(())
    }

    pub fn scan_files(&self, dir: PathBuf, rx: Receiver<bool>) -> Result<(), io::Error> {
        // if spawn fail, not scan files in this directory
        if !rx.recv().map_or_else(|_| false, |v| v) {
            return Ok(());
        }
        let files = fs::read_dir(dir.clone())?
            .filter_map(|dir_entry| dir_entry.ok())
            .filter(|dir_entry| dir_entry.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter(|dir_entry| {
                self.file_patterns.iter().any(|regex| {
                    let path = canonicalize(dir_entry.path().to_path_buf());
                    println!("path:{:?}", path);
                    path.map_or(false, |v| v.to_str().map_or(false, |v| regex.is_match(v)))
                })
            });
        let mut trace_files = self.trace_files.lock().unwrap();
        let files: Vec<_> = files
            .map(|dir_entry| dir_entry.path().to_path_buf())
            .filter(|path_buf| !trace_files.contains_key(path_buf))
            .collect();

        for path_buf in files {
            trace_files.insert(path_buf.clone(), TailPosition::End);
            let sent = self.tx.send((path_buf, TailPosition::End));
            if sent.is_err() {
                todo!("interested file sent failed!");
            }
        }
        Ok(())
    }

    // Scan the root, recursive find all the directory
    // 1.If find a matched directory, watch it with create event, if new file is created send to the channel
    // 2.List all the files below the matched directory, if the files match file pattern,
    //  send to the channel
    pub fn scan_and_watch(&self, root_dirs: Vec<String>) {
        root_dirs
            .iter()
            .flat_map(|root_dir| WalkDir::new(root_dir.as_str()))
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_dir())
            .for_each(|entry| {
                let path = canonicalize(entry.path().to_path_buf());
                path.into_iter().for_each(|path| {
                    self.dir_patterns
                        .iter()
                        .filter(|r| r.is_match(path.to_str().unwrap_or("")))
                        .for_each(|_| {
                            let res = self.spawn_watcher_scan(path.clone());
                            if let Err(e) = res {
                                println!("spawn watch for path failed!path:{}", e);
                            }
                        })
                });
            });
    }
}

#[cfg(test)]
mod tests {
    use super::LogFilesWatcher;
    use notify::{RecommendedWatcher, RecursiveMode, Watcher};
    use regex::bytes::Regex;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use std::sync::mpsc::channel;
    use std::time::Duration;
    use walkdir::WalkDir;

    macro_rules! custom_test {
        (
            #[test(timeout = $timeout:expr)]
            $( #[$meta:meta] )*
            fn $fname:ident $($rest:tt)*
        ) => (
            #[test]
            $( #[$meta] )*
            fn $fname() {
                let (done_tx, done_rx) = ::std::sync::mpsc::channel();
                let handle = ::std::thread::Builder::new()
                    .name(
                        concat!(module_path!(), "::", stringify!($fname))
                        .splitn(2, "::").nth(1).unwrap()
                        .into()
                    )
                    .spawn(move || {
                        {
                            fn $fname $($rest)*
                            $fname()
                        }
                        let _ = done_tx.send(());
                    })
                    .unwrap();
                match done_rx.recv_timeout({
                    use ::std::time::*;
                    $timeout
                }) {
                    | Err(::std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        panic!("Test took too long");
                    },
                    | _ => if let Err(err) = handle.join() {
                        ::std::panic::resume_unwind(err);
                    },
                }
            }
        );

        (
            $($tt:tt)*
        ) => (
            $($tt)*
        );
    }

    #[test]
    fn test_scan_files() {
        let dir_patterns = vec![r".*?/app/logs/.*?".to_string()];
        let file_patterns = vec![r".*?\.testlog".to_string()];
        let (log, mut rx) = LogFilesWatcher::init(dir_patterns, file_patterns);
        // spawn 信号通知
        let (s, r) = channel();

        // 设置要进行scan的目录
        let mut path = PathBuf::new();
        path.push("./test/app/logs/");

        // 发送扫描信息
        let _ = s.send(true);
        let _ = log.scan_files(path, r);
        let mut collected_logs: Vec<String> = vec![];
        for _ in 0..3 {
            let res = rx.blocking_recv();
            let path = res.unwrap().0;
            collected_logs.push(path.to_str().unwrap().to_string());
        }
    }

    #[test]
    fn test_file_watch() {
        let (tx, rx) = channel();
        let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2)).unwrap();
        let _ = watcher.watch("./test", RecursiveMode::NonRecursive);
        loop {
            match rx.recv() {
                Ok(event) => println!("{:?}", event),
                Err(e) => println!("watch error: {:?}", e),
            }
        }
    }

    #[test]
    fn test_walk_dir() {
        for entry in WalkDir::new("./") {
            let entry = entry.unwrap();
            println!("=====");
            println!("{}", entry.path().display());
            let path = canonicalize(entry.path().to_path_buf());
            println!("file_name:{:?}", path);
            println!("=====");
        }
    }

    #[test]
    fn test_files_scan() {
        let dir_pattern = r".*?/app/logs/.*?";
        let regex = Regex::new(dir_pattern).unwrap();
        println!("{}", regex.is_match(b"/asfddsafa/adfasf/app/logs/asdfdasf"));
        println!("{}", regex.is_match(b"adsfsfds/app"));
    }
}
