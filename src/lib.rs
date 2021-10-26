pub mod proto;
pub mod transport;

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
