use regex::Regex;
use std::fs::canonicalize;
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

struct LogFilesWatcher {
    trace_files: HashMap<String, ()>,
    tx: UnboundedSender<String>,
    dir_patterns: Vec<Regex>,
    file_patterns: Vec<Regex>,
    watching_dirs: Vec<PathBuf>,
}

impl LogFilesWatcher {
    fn init(
        dir_patterns: Vec<String>,
        file_patterns: Vec<String>,
    ) -> (LogFilesWatcher, UnboundedReceiver<String>) {
        let dir_patterns = regex_pattern!(dir_patterns);
        let file_patterns = regex_pattern!(file_patterns);
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        (
            LogFilesWatcher {
                trace_files: HashMap::new(),
                tx,
                dir_patterns,
                file_patterns,
                watching_dirs: Vec::new(),
            },
            rx,
        )
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
                            if let Ok(p) = path {}
                        } else {
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
