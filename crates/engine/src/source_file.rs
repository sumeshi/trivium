//! Open user-selected data/configuration files without accepting devices or pipes.
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::Path,
};

fn not_regular() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "Selected path is not a regular file.",
    )
}

/// Follow ordinary file symlinks, but reject directories, FIFOs and devices.
/// The preflight avoids opening a known device; checking the opened handle
/// also covers a file replaced between metadata lookup and open. On Unix,
/// O_NONBLOCK prevents a replacement FIFO from blocking before that check.
pub fn open_regular(path: &Path) -> io::Result<File> {
    if !fs::metadata(path)?.is_file() {
        return Err(not_regular());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(not_regular());
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("trivium-regular-source-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn regular_files_are_read_only_and_directories_are_rejected() {
        let root = Fixture::new();
        let path = root.0.join("source.csv");
        fs::write(&path, b"value\nexact\n").unwrap();
        let mut file = open_regular(&path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"value\nexact\n");
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(
            open_regular(&root.0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(unix)]
    #[test]
    fn fifo_and_devices_are_rejected_without_opening_them_and_regular_symlinks_work() {
        use std::{os::unix::fs::symlink, process::Command};
        let root = Fixture::new();
        let fifo = root.0.join("pipe.csv");
        assert!(Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        assert_eq!(
            open_regular(&fifo).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            open_regular(Path::new("/dev/zero")).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let path = root.0.join("source.csv");
        let alias = root.0.join("alias.csv");
        fs::write(&path, b"value\nexact\n").unwrap();
        symlink(&path, &alias).unwrap();
        let mut bytes = Vec::new();
        open_regular(&alias)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"value\nexact\n");
    }
}
