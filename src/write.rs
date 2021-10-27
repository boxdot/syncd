use std::io;

use sha2::{Digest, Sha256};

pub struct WriterWithShasum<W: io::Write> {
    writer: W,
    hasher: Sha256,
}

impl<W: io::Write> WriterWithShasum<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            hasher: Sha256::new(),
        }
    }

    pub fn finalize(self) -> [u8; 32] {
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
