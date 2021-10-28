use std::fs::File;
use std::io;
use std::path::Path;
use std::pin::Pin;

use memmap2::{Mmap, MmapOptions};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod ignore;
pub mod pathutil;
pub mod proto;
pub mod store;
pub mod transport;
pub mod write;

pub type BoxAsynWrite = Pin<Box<dyn AsyncWrite + Send + Sync>>;
pub type BoxAsynRead = Pin<Box<dyn AsyncRead + Send + Sync>>;

pub fn init<A: argh::TopLevelCommand>() -> A {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info")
    }
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    argh::from_env()
}

pub fn shasum_bytes(data: impl AsRef<[u8]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data.as_ref());
    hasher.finalize().into()
}

pub fn mmap_with_shasum(path: &Path) -> io::Result<(Mmap, [u8; 32])> {
    // Safety: since we assume that files are actively modified all the time, we have
    // to memory map the file as copy-on-write read only.
    //
    // Note: The fd does not have to be kept open:
    //
    // * https://linux.die.net/man/2/mmap
    // * https://pubs.opengroup.org/onlinepubs/7908799/xsh/mmap.html
    let mmap = unsafe { MmapOptions::new().map_copy_read_only(&File::open(path)?)? };
    let shasum = shasum_bytes(&mmap);
    Ok((mmap, shasum))
}

pub fn mmap(path: &Path) -> io::Result<Mmap> {
    let f = File::open(path)?;
    Ok(unsafe { MmapOptions::new().map_copy_read_only(&f)? })
}
