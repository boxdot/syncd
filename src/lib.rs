use sha2::{Digest, Sha256};

pub mod proto;
pub mod transport;
pub mod write;

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
