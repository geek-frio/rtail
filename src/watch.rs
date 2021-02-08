// watch path
// /var/lib/docker/overlay2/${随机}/merged/app/logs
#[cfg(test)]
mod tests {
    use inotify::{Inotify, WatchMask};

    #[test]
    fn test_inotify() {
        let mut inotify = Inotify::init().unwrap();
        inotify
            .add_watch("./test/", WatchMask::CREATE)
            .expect("watch on directory failed!");
    }
}
