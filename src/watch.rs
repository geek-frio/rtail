use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
use std::fs;
use std::fs::canonicalize;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use walkdir::WalkDir;
use notify::DebouncedEvent;
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

    fn watch_dir(&self, dir: PathBuf) {
        let watch_dir = dir.clone();
        let path_tx = self.tx.clone();

        // WE only care about newly created file
        thread::spawn(move || {
            let (tx, rx) = channel();
            let _ =
                Watcher::new(tx, Duration::from_secs(2)).map(|mut watcher: RecommendedWatcher| {
                    watch_dir.to_str().map(|s| {
                        let _ = watcher.watch(s, RecursiveMode::NonRecursive);
                        loop {
                            match rx.recv() {
                                Ok(event) => {
                                    if let DebouncedEvent::Create(path) = event {
                                        if let Ok(path) = canonicalize(path) {
                                            let _ = path_tx.send((path, TailPosition::Start));
                                        }
                                    }
                                },
                                Err(e) => println!("watch error: {:?}", e),
                            }
                        }
                    });
                });
        });
        let _ = fs::read_dir(dir).map(|paths| {
            for path in paths {
                if let Ok(p) = path {
                    let res = p.file_type();
                    let _ = res.map(|ftype| {
                        if ftype.is_file() {
                            for r in self.file_patterns.iter() {
                                let path = canonicalize(p.path().to_path_buf());
                                if let Ok(path) = path {
                                    let sent_path = path.clone();
                                    let path_str = path.to_str();
                                    if let Some(path_str) = path_str {
                                        if r.is_match(path_str) {
                                            let _ = self.trace_files.lock().map(|mut a| {
                                                if a.contains_key(&sent_path) {
                                                    return;
                                                }
                                                a.insert(sent_path.clone(), TailPosition::End);
                                                let _ = self.tx.send((sent_path, TailPosition::End));
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });
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
}
