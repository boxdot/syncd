use std::env::current_dir;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context as _};
use argh::FromArgs;
use fast_rsync::{diff, Signature};
use futures_util::future::poll_fn;
use ignore::{DirEntry, Walk};
use memmap2::MmapOptions;
use notify::{EventKind, RecursiveMode, Watcher};
use syncd::ignore::Ignore;
use syncd::{init, proto, shasum_bytes, transport};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_tower::pipeline;
use tower::Service;
use tracing::{debug, info, warn};
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

    info!("initial sync");
    initial_sync(&dir, &mut client).await?;

    info!(dir = %dir.display(), "watching");
    let (tx, mut rx) = mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event| {
        let _ = tx.blocking_send(event);
    })?;

    watcher
        .watch(&dir, RecursiveMode::Recursive)
        .context("failed to initialize watcher")?;

    let ignore = Ignore::build(&dir)?;

    while let Some(event) = rx.recv().await {
        let event = event.context("watcher failed")?;
        match event.kind {
            EventKind::Create(_) => (),
            EventKind::Modify(_) => (),
            EventKind::Remove(_) => (),
            other => {
                debug!(kind = ?other, "skipping event");
                continue;
            }
        }

        if event.paths.iter().any(|p| ignore.should_skip_path(p)) {
            debug!(?event, "ignore");
            continue;
        }

        info!(?event, "watch notification");
    }

    Ok(())
}

async fn initial_sync<E, S>(dir: &Path, client: &mut S) -> anyhow::Result<()>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    for entry in Walk::new(dir) {
        match entry {
            Ok(entry) => {
                match handle_entry(client, dir, &entry).await {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => {
                        return Err(e); // fatal error
                    }
                    Err(e) => {
                        // handling error
                        warn!(path = %entry.path().display(), reason = %e, "skipping");
                    }
                }
            }
            Err(e) => warn!(reason = %e, "invalid directory entry"),
        }
    }
    Ok(())
}

/// Handles a directory entry via a client conforming the transfer procotol.
///
/// Outer result is the result of handling the entry. It is non-fatal and can be converted into a
/// warning/error.
///
/// The inner (wrapped) result is fatal and comes from the service. It should be considered as
/// non-recoverable.
// TODO: We could use state classes to describe the protocol.
async fn handle_entry<S, E>(
    client: &mut S,
    root: &Path,
    entry: &DirEntry,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    info!(path = %entry.path().display(), "check");

    let metadata = entry.metadata()?;
    let file_type = proto::FileType::from_fs(metadata.file_type())
        .ok_or_else(|| anyhow!("unknown file type"))?;

    let (transfer, mmap) = match file_type {
        proto::FileType::Dir => (None, None),
        proto::FileType::File => {
            // Safely: since we assume that files are actively modified all the time, we have
            // to memory map the file as copy-on-write.
            //
            // Note: The fd does not have to be kept open:
            //
            // * https://linux.die.net/man/2/mmap
            // * https://pubs.opengroup.org/onlinepubs/7908799/xsh/mmap.html
            let mmap = unsafe { MmapOptions::new().map_copy(&File::open(entry.path())?)? };
            let shasum = shasum_bytes(&mmap);
            (
                Some(proto::Transfer {
                    data: Default::default(),
                    kind: proto::TransferKind::Empty,
                    shasum,
                    len: None,
                }),
                Some(mmap),
            )
        }
        proto::FileType::Symlink => bail!("symlink unimplemented"),
    };
    let shasum = transfer.as_ref().map(|t| t.shasum);

    let relative_path = entry.path().strip_prefix(root)?;
    let mut req = proto::TransferRequest {
        id: Uuid::new_v4(),
        path: relative_path.into(),
        file_type,
        kind: proto::TransferRequestKind::Check,
        transfer,
    };

    // protocol has max 2 requests depth
    for _ in 1..3 {
        let resp = match call(client, req).await {
            Ok(resp) => resp,
            Err(e) => return Ok(Err(e.into())),
        };

        req = match resp.kind {
            proto::TransferResponseKind::Exists => return Ok(Ok(())),
            proto::TransferResponseKind::Created => {
                info!(path = %entry.path().display(), "created");
                return Ok(Ok(()));
            }
            proto::TransferResponseKind::ExistsDifferent { signature } => {
                let mmap = mmap
                    .as_ref()
                    .expect("logic error: mmap not set for file transfer");
                let shasum = shasum.expect("logic error: shasum not set for for file transfer");

                let sig = Signature::deserialize(&signature)?;
                let mut delta = Vec::new();
                diff(&sig.index(), &mmap, &mut delta)?;

                proto::TransferRequest {
                    id: Uuid::new_v4(),
                    path: relative_path.into(),
                    file_type,
                    kind: proto::TransferRequestKind::Delta,
                    transfer: Some(proto::Transfer {
                        kind: proto::TransferKind::Delta,
                        data: delta,
                        shasum,
                        len: Some(mmap.len()),
                    }),
                }
            }
            proto::TransferResponseKind::NeedContents => {
                let mmap = mmap
                    .as_ref()
                    .expect("logic error: mmap not set for file transfer");
                let shasum = shasum.expect("logic error: shasum not set for for file transfer");
                proto::TransferRequest {
                    id: Uuid::new_v4(),
                    path: relative_path.into(),
                    file_type,
                    kind: proto::TransferRequestKind::Contents,
                    transfer: Some(proto::Transfer {
                        kind: proto::TransferKind::Contents,
                        data: mmap.to_vec(),
                        shasum,
                        len: None,
                    }),
                }
            }
            proto::TransferResponseKind::CantHandle { reason } => {
                bail!("handler failed: {}", reason);
            }
        };
    }

    Ok(Err(anyhow!("giving up")))
}

async fn call<Req, S>(svc: &mut S, req: Req) -> Result<S::Response, S::Error>
where
    Req: Debug,
    S: Service<Req>,
    S::Response: Debug,
    S::Error: Debug,
{
    debug!(?req, "send");
    poll_fn(|cx| svc.poll_ready(cx)).await?;
    let resp = svc.call(req).await;
    debug!(?resp, "received");
    resp
}
