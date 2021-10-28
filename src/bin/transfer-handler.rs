use std::convert::Infallible;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use anyhow::{anyhow, bail, Context as _};
use argh::FromArgs;
use fast_rsync::{apply_limited, Signature, SignatureOptions};
use futures_util::FutureExt;
use syncd::proto::{
    FileType, Transfer, TransferKind, TransferRequest, TransferResponse, TransferResponseKind,
};
use syncd::store::Store;
use syncd::write::WriterWithShasum;
use syncd::{init, mmap, mmap_with_shasum, proto, transport, BoxAsynRead, BoxAsynWrite};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tower::pipeline;
use tracing::{debug, info};

/// Handler of transfer requests
#[derive(Debug, FromArgs)]
struct Args {
    #[argh(positional)]
    /// root of the transfer handler
    root: PathBuf,
    /// instead of communicating via stdin/stdout listen on a socket
    #[argh(option)]
    listen: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = init();

    info!("waiting for connection");

    loop {
        let (read, write): (BoxAsynRead, BoxAsynWrite) = if let Some(listen) = args.listen.as_ref()
        {
            let listener = TcpListener::bind(listen).await?;
            let (socket, _addr) = listener.accept().await?;
            let (read, write) = socket.into_split();
            (Box::pin(read), (Box::pin(write)))
        } else {
            (Box::pin(tokio::io::stdin()), Box::pin(tokio::io::stdout()))
        };

        info!("connection accepted");

        let transport = transport::BincodeTransport::<
            proto::TransferRequest,
            proto::TransferResponse,
            _,
            _,
        >::new(read, write);

        if !args.root.exists() {
            fs::create_dir_all(&args.root)?;
        } else if !args.root.is_dir() {
            bail!("{} exists and is not a directory", args.root.display());
        }

        let cx = TransferHandlerContext {
            root: Arc::new(args.root.clone()),
            store: Default::default(),
        };

        let service = tower::service_fn(move |req| {
            transfer_handler(cx.clone(), req).map(Ok::<_, Infallible>)
        });

        info!("running handler");

        pipeline::Server::new(transport, service)
            .await
            .map_err(|e| anyhow!(e.to_string()))
            .context("handle-transfer server failed")?;

        if args.listen.is_none() {
            break;
        }
    }

    info!("shutting down");

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct TransferHandlerContext {
    root: Arc<PathBuf>,
    store: Arc<Mutex<Store>>,
}

async fn transfer_handler(cx: TransferHandlerContext, req: TransferRequest) -> TransferResponse {
    debug!(request = ?req, "incoming");

    let id = req.id;
    let resp = match req.kind {
        proto::TransferRequestKind::Check => handle_check(cx, req),
        proto::TransferRequestKind::Delta => handle_delta(cx, req).await,
        proto::TransferRequestKind::Contents => handle_contents(cx, req).await,
        proto::TransferRequestKind::Remove => handle_remove(&cx.root, req),
        proto::TransferRequestKind::Rename { .. } => handle_rename(&cx.root, req),
    };

    let resp = resp.unwrap_or_else(|e| TransferResponse {
        id,
        kind: TransferResponseKind::CantHandle {
            reason: e.to_string(),
        },
    });

    debug!(response = ?resp, "sending");
    resp
}

fn handle_check(
    cx: TransferHandlerContext,
    req: TransferRequest,
) -> anyhow::Result<TransferResponse> {
    let path = cx.root.join(req.path);
    match req.file_type {
        FileType::Dir => {
            let kind = handle_check_dir(&path).unwrap_or_else(From::from);
            Ok(TransferResponse { id: req.id, kind })
        }
        FileType::File => {
            let transfer = req
                .transfer
                .ok_or_else(|| anyhow!("missing transfer data on check file request"))?;
            let kind = handle_check_file(&path, transfer).unwrap_or_else(From::from);
            Ok(TransferResponse { id: req.id, kind })
        }
        FileType::Symlink => {
            bail!("symlinks are not implemented");
        }
    }
}

fn handle_check_dir(path: &Path) -> io::Result<TransferResponseKind> {
    if path.exists() {
        if !path.is_dir() {
            fs::remove_file(&path)?;
            fs::create_dir_all(path)?;
        }
    } else {
        fs::create_dir_all(path)?;
    }
    Ok(TransferResponseKind::Ok)
}

fn handle_check_file(path: &Path, transfer: Transfer) -> io::Result<TransferResponseKind> {
    debug!(
        "handle_check_file at {} with transfer {:?}",
        path.display(),
        transfer
    );
    if !path.exists() {
        Ok(TransferResponseKind::NeedContents)
    } else {
        let (mmap, shasum) = mmap_with_shasum(path)?;

        if shasum == transfer.shasum {
            Ok(TransferResponseKind::Ok)
        } else {
            // TODO: Reuse buffers
            let mut storage = Vec::new();
            let mut signature = Vec::new();
            let signature_options = SignatureOptions {
                block_size: 4096,
                crypto_hash_size: 8,
            };
            Signature::calculate(&mmap, &mut storage, signature_options).serialize(&mut signature);
            Ok(TransferResponseKind::Different { signature })
        }
    }
}

async fn handle_contents(
    cx: TransferHandlerContext,
    req: TransferRequest,
) -> anyhow::Result<TransferResponse> {
    if req.file_type != FileType::File {
        bail!("contents request for a non-file");
    }
    let transfer = req
        .transfer
        .ok_or_else(|| anyhow!("transfer data missing for contents request"))?;
    if transfer.kind != TransferKind::Contents {
        bail!("transfer kind is not contents for contents request");
    }
    let file_size = transfer
        .file_size
        .ok_or_else(|| anyhow!("contents transfer does not have file_size"))?;

    let path = cx.root.join(req.path);
    debug!(path = %path.display(), file_size, "handle_contents");

    let mut store = cx.store.lock().await;
    let total_bytes = store
        .push_file_chunk(path.clone(), transfer.shasum, &transfer.data)
        .await?;
    if total_bytes == file_size as u64 {
        // we got the last chunk
        let shasum = store
            .remove_file(path)
            .await?
            .expect("logic error: file not in store");
        if shasum != transfer.shasum {
            bail!(
                "data integrity failed: {} vs expected {}",
                hex::encode(shasum),
                hex::encode(transfer.shasum)
            );
        }
    }

    Ok(TransferResponse {
        id: req.id,
        kind: TransferResponseKind::Ok,
    })
}

async fn handle_delta(
    cx: TransferHandlerContext,
    req: TransferRequest,
) -> anyhow::Result<TransferResponse> {
    if req.file_type != FileType::File {
        bail!("delta request for a non-file");
    }
    let transfer = req
        .transfer
        .ok_or_else(|| anyhow!("transfer data missing for contents request"))?;
    if transfer.kind != TransferKind::Delta {
        bail!("transfer kind is not delta for delta request");
    }
    let file_size = transfer
        .file_size
        .ok_or_else(|| anyhow!("delta transfer does not have file_size"))?;
    let data_size = transfer
        .data_size
        .ok_or_else(|| anyhow!("delta transfer does not have data_size"))?;

    let path = cx.root.join(req.path);

    // TODO: optimize the case where the is only a single chunk
    let mut store = cx.store.lock().await;
    let delta = store.push_delta_chunk(path.clone(), transfer.shasum, &transfer.data);

    if delta.len() != data_size {
        // need more delta chunks
        return Ok(TransferResponse {
            id: req.id,
            kind: TransferResponseKind::Ok,
        });
    }

    // we got the last delta chunk
    store.remove_delta(&path);
    drop(store);

    let mmap = mmap(&path)?;
    fs::remove_file(&path)?; // unlink previous file to avoid overriding the mmap

    let f = File::create(&path)?;
    let mut out = WriterWithShasum::new(BufWriter::new(f));
    apply_limited(&mmap, &transfer.data, &mut out, file_size)?;
    let shasum = out.finalize();

    if shasum == transfer.shasum {
        // apply worked
        Ok(TransferResponse {
            id: req.id,
            kind: TransferResponseKind::Ok,
        })
    } else {
        // apply failed => ask for the full contents
        Ok(TransferResponse {
            id: req.id,
            kind: TransferResponseKind::NeedContents,
        })
    }
}

fn handle_remove(root: &Path, req: TransferRequest) -> anyhow::Result<TransferResponse> {
    // Assumption: if we remove a dir, then all files were removed before by other requests.
    // This is not true, if requests are multiplexed, which is not the case atm.

    let path = root.join(req.path);

    match req.file_type {
        FileType::Dir => fs::remove_dir(path)?,
        FileType::File | FileType::Symlink => fs::remove_file(path)?,
    }

    Ok(TransferResponse {
        id: req.id,
        kind: TransferResponseKind::Ok,
    })
}

fn handle_rename(root: &Path, req: TransferRequest) -> anyhow::Result<TransferResponse> {
    let from = root.join(req.path);
    let to = match req.kind {
        proto::TransferRequestKind::Rename { new_path } => root.join(new_path),
        _ => bail!("unexpected request kind in rename"),
    };

    if to.exists() {
        if to.is_dir() {
            fs::remove_dir_all(&to)?;
        } else {
            fs::remove_file(&to)?;
        }
        fs::rename(from, to)?;
    }

    Ok(TransferResponse {
        id: req.id,
        kind: TransferResponseKind::Ok,
    })
}
