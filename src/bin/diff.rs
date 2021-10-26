use std::fs::File;
use std::io::{self, BufWriter};
use std::path::PathBuf;

use anyhow::Context as _;
use argh::FromArgs;
use fast_rsync::{apply_limited, diff, Signature, SignatureOptions};
use memmap2::MmapOptions;
use sha2::{Digest, Sha256};
use syncd::init;
use tracing::info;

/// Sync files from A to B over ssh in real time
#[derive(Debug, FromArgs)]
struct Args {
    /// file to read
    #[argh(positional)]
    from: PathBuf,
    #[argh(positional)]
    /// file to update
    to: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args: Args = init();

    // signature of to
    let to_data = std::fs::read(&args.to)?;

    let mut storage = Vec::new();
    let signature_options = SignatureOptions {
        block_size: 4096,
        crypto_hash_size: 8,
    };
    let sig = Signature::calculate(&to_data, &mut storage, signature_options);
    let indexed_sig = sig.index();

    // delta
    let from_file = File::open(&args.from)?;
    let from_mmap = unsafe { MmapOptions::new().map(&from_file)? };
    let mut hasher = Sha256::new();
    hasher.update(&from_mmap);
    let from_shasum: [u8; 32] = hasher.finalize().into();

    let mut delta = Vec::new();
    diff(&indexed_sig, &from_mmap, &mut delta).context("failed to calculate diff")?;

    info!(bytes = from_mmap.len(), "from size");
    info!(bytes = to_data.len(), "to size");
    info!(bytes = delta.len(), "delta size");

    // apply
    let mut out = WriterWithShasum::new(BufWriter::new(File::create(args.to)?));
    apply_limited(&to_data, &delta, &mut out, from_mmap.len()).context("failed to apply delta")?;
    let to_shasum = out.finalize();

    info!("from == to: {}", from_shasum == to_shasum);

    Ok(())
}

struct WriterWithShasum<W: io::Write> {
    writer: W,
    hasher: Sha256,
}

impl<W: io::Write> WriterWithShasum<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            hasher: Sha256::new(),
        }
    }

    fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl<W: io::Write> io::Write for WriterWithShasum<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
