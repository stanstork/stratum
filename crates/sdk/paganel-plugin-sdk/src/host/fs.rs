use std::io;
use std::path::Path;

/// Read a file's raw bytes. Limited to `allow_fs_read` directories.
pub fn fs_read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    std::fs::read(path)
}

/// Read a file as UTF-8 text. Limited to `allow_fs_read` directories.
pub fn fs_read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    std::fs::read_to_string(path)
}

/// Write bytes to a file. Limited to `allow_fs_write` directories.
pub fn fs_write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    std::fs::write(path, contents)
}
