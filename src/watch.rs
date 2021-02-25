use notify::DebouncedEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
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

enum TailPosition {
    Start,
    End,
}

struct LogFilesWatcher {
    trace_files: Arc<Mutex<HashMap<PathBuf, TailPosition>>>,
    tx: UnboundedSender<(PathBuf, TailPosition)>,
    dir_patterns: Vec<Regex>,
    file_patterns: Vec<Regex>,
    watching_dirs: Vec<PathBuf>,
}

impl LogFilesWatcher {
    fn init(
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
                watching_dirs: Vec::new(),
            },
            rx,
        )
    }

    fn spawn_watcher(&self, watch_dir: PathBuf, notify: Sender<()>) -> Result<(), io::Error> {
        let path_tx = self.tx.clone();
        thread::spawn(move || {
            let (tx, rx) = channel();
            Watcher::new(tx, Duration::from_secs(2)).map(|mut watcher: RecommendedWatcher| {
                watch_dir.to_str().map(|s| {
                    watcher.watch(s, RecursiveMode::NonRecursive);
                    // 进入watch通知扫描
                    notify.send(());
                    rx.iter().for_each(|event| {
                        if let DebouncedEvent::Create(path) = event {
                            if let Ok(path) = canonicalize(path) {
                                let _ = path_tx.send((path, TailPosition::Start));
                            }
                        }
                    })
                });
            });
        });
        Ok(())
    }

    fn watch_dir(&self, dir: PathBuf, rx: Receiver<()>) -> Result<(), io::Error> {
        let _ = rx.recv();
        let files = fs::read_dir(dir.clone())?
            .filter_map(|dir_entry| dir_entry.ok())
            .filter(|dir_entry| dir_entry.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter(|dir_entry| {
                self.file_patterns.iter().any(|regex| {
                    let path = canonicalize(dir_entry.path().to_path_buf());
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
    fn scan_and_watch(&self, root_dirs: Vec<String>) {
        for root_dir in root_dirs {
            for entry in WalkDir::new(root_dir.as_str()) {
                match entry {
                    Ok(entry) => {
                        if entry.file_type().is_dir() {
                            let path = canonicalize(entry.path().to_path_buf());
                            if let Ok(p) = path {
                                let mut matched = false;
                                for r in self.dir_patterns.iter() {
                                    if r.is_match(p.to_str().unwrap_or("")) {
                                        matched = true;
                                    }
                                }
                                if matched {
                                    self.watch_dir(p);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Have met error when scan, error:{:?}", e);
                    }
                }
            }
        }
    }
}

// watch path
// /var/lib/docker/overlay2/${container_id}/merged/app/logs

#[cfg(test)]
mod tests {
    use notify::{RecommendedWatcher, RecursiveMode, Watcher};
    use regex::bytes::Regex;
    use std::fs::canonicalize;
    use std::sync::mpsc::channel;
    use std::time::Duration;
    use walkdir::WalkDir;

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
