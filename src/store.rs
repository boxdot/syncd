use std::collections::{hash_map, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::{fs, io};

/// Asynchronous store for open files and deltas.
///
/// Accumulates chunks of data in the store. Files data chunks are hashed with sha256 hasher.
#[derive(Debug, Default)]
pub struct Store {
    files: HashMap<PathBuf, FileEntry>,
    deltas: HashMap<PathBuf, Vec<u8>>,
}

impl Store {
    /// Returns the number of total bytes written to the file so far.
    pub async fn push_file_chunk(&mut self, path: PathBuf, data: &[u8]) -> io::Result<u64> {
        let mut entry = self.files.entry(path.clone());
        let mut file_entry = match entry {
            hash_map::Entry::Occupied(ref mut entry) => entry.get_mut(),
            hash_map::Entry::Vacant(entry) => entry.insert(FileEntry::new(&path).await?),
        };
        file_entry.write_all(data).await?;
        file_entry.num_bytes += data.len() as u64;
        Ok(file_entry.num_bytes)
    }

    pub fn push_delta_chunk(&mut self, path: PathBuf, data: &[u8]) -> &[u8] {
        let delta = self.deltas.entry(path).or_default();
        delta.extend(data);
        delta
    }

    /// Returns the sha256 sum of the file if the file was in the store.
    pub async fn remove_file(&mut self, path: PathBuf) -> io::Result<Option<[u8; 32]>> {
        Ok(match self.files.entry(path) {
            hash_map::Entry::Occupied(entry) => {
                let mut file_entry = entry.remove();
                file_entry.flush().await?;
                let shasum = file_entry.hasher.finalize().into();
                Some(shasum)
            }
            hash_map::Entry::Vacant(_) => None,
        })
    }

    pub fn remove_delta(&mut self, path: &Path) {
        self.deltas.remove(path);
    }
}

#[pin_project]
#[derive(Debug)]
struct FileEntry {
    #[pin]
    f: io::BufWriter<fs::File>,
    hasher: Sha256,
    num_bytes: u64,
}

impl FileEntry {
    pub async fn new(path: &Path) -> io::Result<Self> {
        Ok(Self {
            f: io::BufWriter::new(fs::File::create(path).await?),
            hasher: Default::default(),
            num_bytes: 0,
        })
    }
}

impl AsyncWrite for FileEntry {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        let res = this.f.poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = res {
            this.hasher.update(&buf[..n])
        }
        res
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.project().f.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.project().f.poll_shutdown(cx)
    }
}
