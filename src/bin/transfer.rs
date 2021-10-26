use std::env::current_dir;
use std::fmt::Debug;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, Context as _};
use argh::FromArgs;
use futures_util::future::poll_fn;
use ignore::Walk;
use syncd::{init, proto, transport};
use tokio::process::Command;
use tokio_tower::pipeline;
use tower::Service;
use tracing::{info, warn};
use uuid::Uuid;

/// Transfer directory structure via transfer-handler
#[derive(Debug, FromArgs)]
struct Args {
    /// client command to start
    #[argh(option)]
    handler_cmd: String,
    #[argh(option)]
    /// directory to transfer [default: current working directory]
    root: Option<PathBuf>,
    /// where to transfer files
    #[argh(positional)]
    dest: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = init();

    let mut client = Command::new(&args.handler_cmd)
        .arg(&args.dest)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdin = client
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open stdin of client"))?;
    let stdout = client
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to open stdout of client"))?;
    let transport =
        transport::BincodeTransport::<proto::TransferResponse, proto::TransferRequest, _, _>::new(
            stdout, stdin,
        );
    let mut client = pipeline::Client::<_, tokio_tower::Error<_, _>, _>::new(transport);

    let dir = args
        .root
        .map(Ok)
        .unwrap_or_else(current_dir)
        .context("failed to use current working directory as root")?;

    for result in Walk::new(dir) {
        match result {
            Ok(entry) => {
                info!("checking path: {}", entry.path().display());
                match entry.metadata() {
                    Ok(metadata) => {
                        let ft = metadata.file_type();
                        let file_type = if ft.is_dir() {
                            proto::FileType::Dir
                        } else if ft.is_file() {
                            proto::FileType::File
                        } else if ft.is_symlink() {
                            proto::FileType::Symlink
                        } else {
                            warn!(
                                "skipping {} due to unknown file type",
                                entry.path().display()
                            );
                            continue;
                        };
                        let req = proto::TransferRequest {
                            id: Uuid::new_v4(),
                            path: entry.path().into(),
                            file_type,
                            kind: proto::TransferRequestKind::Check,
                            transfer: Default::default(),
                        };

                        info!("request {:?}", req);

                        if let Err(e) = service_ready(&mut client).await {
                            warn!(
                                "skipping {} due to failed ready request: {}",
                                entry.path().display(),
                                e
                            );
                        }
                        let resp = client.call(req).await;

                        info!("response: {:?}", resp);
                    }
                    Err(e) => {
                        warn!(
                            "skipping {} due to invalid metadata: {}",
                            entry.path().display(),
                            e
                        );
                        continue;
                    }
                }
            }
            Err(e) => warn!("{}", e),
        }
    }

    Ok(())
}

async fn service_ready<S: Service<Request>, Request>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}
