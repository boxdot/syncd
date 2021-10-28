use std::env::current_dir;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context as _};
use argh::FromArgs;
use fast_rsync::{diff, Signature};
use futures_util::future::poll_fn;
use ignore::{DirEntry, WalkBuilder};
use memmap2::Mmap;
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use syncd::ignore::Ignore;
use syncd::{init, mmap_with_shasum, proto, transport, BoxAsynRead, BoxAsynWrite};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_tower::pipeline;
use tower::Service;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const FILE_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

/// Transfer directory structure via transfer-handler
#[derive(Debug, FromArgs)]
struct Args {
    /// client command to start
    #[argh(option)]
    handler_cmd: Option<String>,
    /// where to transfer files
    #[argh(option)]
    dest: Option<PathBuf>,
    /// TCP socket to connect to
    #[argh(option)]
    connect: Option<String>,
    #[argh(option)]
    /// directory to transfer [default: current working directory]
    root: Option<PathBuf>,
    /// include hidden files and directories
    #[argh(switch)]
    hidden: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = init();

    let (read, write): (BoxAsynRead, BoxAsynWrite) = if let Some(handler_cmd) = args.handler_cmd {
        let dest = args
            .dest
            .ok_or_else(|| anyhow!("--dest has to be provided when using --handler-cmd"))?;
        let mut client = Command::new(&handler_cmd)
            .arg(dest)
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
        (Box::pin(stdout), Box::pin(stdin))
    } else if let Some(connect) = args.connect {
        let stream = TcpStream::connect(connect).await?;
        let (read, write) = stream.into_split();
        (Box::pin(read), Box::pin(write))
    } else {
        bail!("either --handler-cmd or --socket must be specified");
    };

    let transport =
        transport::BincodeTransport::<proto::TransferResponse, proto::TransferRequest, _, _>::new(
            read, write,
        );
    let mut client = pipeline::Client::<_, tokio_tower::Error<_, _>, _>::with_error_handler(
        transport,
        |e| error!(reason = %e, "client failed"),
    );

    let dir = args
        .root
        .map(Ok)
        .unwrap_or_else(current_dir)
        .context("failed to use current working directory as root")?;
    let dir = dir.canonicalize()?;

    info!("initial sync");
    initial_sync(&dir, &mut client, args.hidden).await?;

    let (tx, mut rx) = mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event| {
        let _ = tx.blocking_send(event);
    })?;

    watcher
        .watch(&dir, RecursiveMode::Recursive)
        .context("failed to initialize watcher")?;
    info!(dir = %dir.display(), "watching");

    let ignore = Ignore::build(&dir, !args.hidden)?;
    debug!(?ignore, "ignore list");

    while let Some(event) = rx.recv().await {
        let event = event.context("watcher failed")?;
        match handle_fs_event(&mut client, &ignore, event, &dir).await {
            Ok(Ok(())) => (),
            Ok(e) => return e, // fatal error
            Err(e) => {
                // handling error
                warn!(reason = %e, "event handler failed");
            }
        }
    }
    Ok(())
}

async fn handle_fs_event<E, S>(
    client: &mut S,
    ignore: &Ignore,
    event: Event,
    root: &Path,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let mut paths = event.paths.clone().into_iter();
    let p1 = paths.next();
    let p2 = paths.next();

    match (event.kind.clone(), p1, p2) {
        (EventKind::Create(CreateKind::Folder), Some(path), _)
            if !ignore.should_skip_path(&path) =>
        {
            info!(path = %path.display(), "create dir");
            check_dir(client, root, &path).await
        }
        (EventKind::Create(CreateKind::File), Some(path), _) if !ignore.should_skip_path(&path) => {
            info!(path = %path.display(), "create file");
            transfer_contents(client, root, &path).await
        }
        (EventKind::Modify(ModifyKind::Data(_)), Some(path), _)
            if !ignore.should_skip_path(&path) =>
        {
            info!(path = %path.display(), "modify");
            check_file(client, root, &path).await
        }
        (EventKind::Modify(ModifyKind::Name(RenameMode::Both)), Some(from), Some(to)) => {
            let is_dir = from.is_dir();
            if ignore.should_skip_path(&from) && !ignore.should_skip_path(&to) {
                if is_dir {
                    info!(path = %to.display(), "create dir");
                    check_dir(client, root, &to).await
                } else {
                    info!(path = %to.display(), "modify");
                    check_file(client, root, &to).await
                }
            } else if !ignore.should_skip_path(&to) {
                info!(from = %from.display(), to = %to.display(), "rename");
                handle_event_rename(client, from, to).await
            } else {
                debug!(?event, "skipping");
                Ok(Ok(()))
            }
        }
        (EventKind::Remove(RemoveKind::Folder), Some(path), _)
            if !ignore.should_skip_path(&path) =>
        {
            info!(path = %path.display(), "remove dir");
            handle_event_remove(client, path, true).await
        }
        (EventKind::Remove(RemoveKind::File), Some(path), _) if !ignore.should_skip_path(&path) => {
            info!(path = %path.display(), "remove file");
            handle_event_remove(client, path, false).await
        }
        _ => {
            debug!(?event, "skipping");
            Ok(Ok(()))
        }
    }
}

async fn handle_event_remove<E, S>(
    client: &mut S,
    path: PathBuf,
    is_dir: bool,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let file_type = if is_dir {
        proto::FileType::Dir
    } else {
        proto::FileType::File
    };
    let req = proto::TransferRequest {
        id: Uuid::new_v4(),
        path,
        file_type,
        kind: proto::TransferRequestKind::Remove,
        transfer: None,
    };
    send_request(client, req).await
}

async fn handle_event_rename<E, S>(
    client: &mut S,
    from: PathBuf,
    to: PathBuf,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let req = proto::TransferRequest {
        id: Uuid::new_v4(),
        path: from,
        file_type: proto::FileType::File, // does not matter
        kind: proto::TransferRequestKind::Rename { new_path: to },
        transfer: None,
    };
    send_request(client, req).await
}

async fn initial_sync<E, S>(dir: &Path, client: &mut S, include_hidden: bool) -> anyhow::Result<()>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let mut builder = WalkBuilder::new(dir);
    builder.hidden(!include_hidden);
    let walk = builder.build();

    for entry in walk {
        match entry {
            Ok(entry) => {
                match handle_entry(client, dir, &entry).await {
                    Ok(Ok(())) => (),
                    Ok(e) => return e, // fatal error
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
async fn handle_entry<S, E>(
    client: &mut S,
    root: &Path,
    entry: &DirEntry,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let path = entry.path();
    let metadata = entry.metadata()?;
    let file_type = proto::FileType::from_fs(metadata.file_type())
        .ok_or_else(|| anyhow!("unknown file type"))?;
    match file_type {
        proto::FileType::Dir => {
            info!(path = %path.display(), "transfer dir");
            check_dir(client, root, path).await
        }
        proto::FileType::File => {
            info!(path = %path.display(), "transfer file");
            check_file(client, root, path).await
        }
        proto::FileType::Symlink => bail!("symlinks are not supported"),
    }
}

async fn check_dir<S, E>(
    client: &mut S,
    root: &Path,
    path: &Path,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let relative_path = path.strip_prefix(root)?;
    let req = proto::TransferRequest {
        id: Uuid::new_v4(),
        path: relative_path.into(),
        file_type: proto::FileType::Dir,
        kind: proto::TransferRequestKind::Check,
        transfer: None,
    };

    send_request(client, req).await
}

async fn check_file<S, E>(
    client: &mut S,
    root: &Path,
    path: &Path,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let (mmap, shasum) = mmap_with_shasum(path)?;

    let relative_path = path.strip_prefix(root)?;

    let transfer = proto::Transfer {
        data: Vec::new(),
        kind: proto::TransferKind::Empty,
        shasum,
        file_size: None,
        data_size: None,
    };
    let req = proto::TransferRequest {
        id: Uuid::new_v4(),
        path: relative_path.into(),
        file_type: proto::FileType::File,
        kind: proto::TransferRequestKind::Check,
        transfer: Some(transfer),
    };

    let resp = match send(client, req).await {
        Ok(resp) => resp,
        Err(e) => return Ok(Err(e.into())),
    };

    match resp.kind {
        proto::TransferResponseKind::Ok => Ok(Ok(())),
        proto::TransferResponseKind::Different { signature } => {
            transfer_delta_with_mmap(client, root, path, mmap, shasum, signature).await
        }
        proto::TransferResponseKind::NeedContents => {
            transfer_contents_with_mmap(client, root, path, mmap, shasum).await
        }
        proto::TransferResponseKind::CantHandle { reason } => {
            bail!("handler failed: {}", reason);
        }
    }
}

async fn transfer_contents<S, E>(
    client: &mut S,
    root: &Path,
    path: &Path,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let (mmap, shasum) = mmap_with_shasum(path)?;
    transfer_contents_with_mmap(client, root, path, mmap, shasum).await
}

async fn transfer_contents_with_mmap<S, E>(
    client: &mut S,
    root: &Path,
    path: &Path,
    mmap: Mmap,
    shasum: [u8; 32],
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let relative_path = path.strip_prefix(root)?;

    for (n, chunk) in mmap.chunks(FILE_CHUNK_SIZE).enumerate() {
        debug!(path = %path.display(), chunk = n, "transfer chunk");
        let file_size = mmap.len();
        let transfer = proto::Transfer {
            data: chunk.to_vec(),
            kind: proto::TransferKind::Contents,
            shasum,
            file_size: Some(file_size),
            data_size: Some(file_size),
        };
        let req = proto::TransferRequest {
            id: Uuid::new_v4(),
            path: relative_path.into(),
            file_type: proto::FileType::File,
            kind: proto::TransferRequestKind::Contents,
            transfer: Some(transfer),
        };

        if let Err(e) = send_request(client, req).await? {
            return Ok(Err(e));
        }
    }

    Ok(Ok(()))
}

async fn transfer_delta_with_mmap<S, E>(
    client: &mut S,
    root: &Path,
    path: &Path,
    mmap: Mmap,
    shasum: [u8; 32],
    signature: Vec<u8>,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    let sig = Signature::deserialize(&signature)?;
    let mut delta = Vec::new();
    diff(&sig.index(), &mmap, &mut delta)?;

    let relative_path = path.strip_prefix(root)?;

    let chunks = delta.chunks(FILE_CHUNK_SIZE);
    let num_chunks = chunks.len();

    let mut needs_contents = false;

    for (n, chunk) in chunks.enumerate() {
        debug!(path = %path.display(), chunk = n, "transfer chunk");
        let transfer = proto::Transfer {
            kind: proto::TransferKind::Delta,
            data: chunk.to_vec(),
            shasum,
            file_size: Some(mmap.len()),
            data_size: Some(delta.len()),
        };
        let req = proto::TransferRequest {
            id: Uuid::new_v4(),
            path: relative_path.into(),
            file_type: proto::FileType::File,
            kind: proto::TransferRequestKind::Delta,
            transfer: Some(transfer),
        };

        match send(client, req).await {
            Ok(proto::TransferResponse {
                kind: proto::TransferResponseKind::Ok,
                ..
            }) => (),
            Ok(proto::TransferResponse {
                kind: proto::TransferResponseKind::NeedContents,
                ..
            }) if n + 1 == num_chunks => {
                // apply delta failed
                needs_contents = true;
            }
            Ok(proto::TransferResponse {
                kind: proto::TransferResponseKind::CantHandle { reason },
                ..
            }) => bail!("handler failed: {}", reason),
            Ok(resp) => {
                return Ok(Err(anyhow!(
                    "protocol violation: got {:?} for chunk {}/{}",
                    resp.kind,
                    n,
                    num_chunks
                )))
            }
            Err(e) => return Ok(Err(e.into())),
        }
    }

    if needs_contents {
        transfer_contents_with_mmap(client, root, path, mmap, shasum).await
    } else {
        Ok(Ok(()))
    }
}

/// Sends requests and waits for success response.
///
/// If the client fails, or protocol is violated, returns an inner error. When client received the
/// request, but reports an error for handling it, returns it as an outer error.
async fn send_request<E, S>(
    client: &mut S,
    req: proto::TransferRequest,
) -> anyhow::Result<anyhow::Result<()>>
where
    E: std::error::Error + Sync + Send + 'static,
    S: Service<proto::TransferRequest, Response = proto::TransferResponse, Error = E>,
{
    Ok(match send(client, req).await {
        Ok(proto::TransferResponse {
            kind: proto::TransferResponseKind::Ok,
            ..
        }) => Ok(()),
        Ok(proto::TransferResponse {
            kind: proto::TransferResponseKind::CantHandle { reason },
            ..
        }) => bail!("handler failed: {}", reason),
        Ok(resp) => Err(anyhow!("protocol violation: got {:?}", resp.kind)),
        Err(e) => Err(e.into()),
    })
}

async fn send<Req, S>(svc: &mut S, req: Req) -> Result<S::Response, S::Error>
where
    Req: Debug,
    S: Service<Req>,
    S::Response: Debug,
    S::Error: Debug,
{
    // trace!(?req, "send");
    poll_fn(|cx| svc.poll_ready(cx)).await?;
    let resp = svc.call(req).await;
    // trace!(?resp, "received");
    resp
}
