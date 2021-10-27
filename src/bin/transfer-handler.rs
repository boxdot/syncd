use std::convert::Infallible;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::{fs, io};

use anyhow::{anyhow, bail, Context as _};
use argh::FromArgs;
use fast_rsync::{apply_limited, Signature, SignatureOptions};
use futures_util::FutureExt;
use memmap2::MmapOptions;
use syncd::proto::{
    FileType, Transfer, TransferKind, TransferRequest, TransferResponse, TransferResponseKind,
};
use syncd::write::WriterWithShasum;
use syncd::{init, proto, shasum_bytes, transport};
use tokio_tower::pipeline;
use tracing::debug;

/// Handler of transfer requests
#[derive(Debug, FromArgs)]
struct Args {
    #[argh(positional)]
    /// root of the transfer handler
    root: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = init();

    let transport =
        transport::BincodeTransport::<proto::TransferRequest, proto::TransferResponse, _, _>::new(
            tokio::io::stdin(),
            tokio::io::stdout(),
        );

    if !args.root.exists() {
        fs::create_dir_all(&args.root)?;
    } else if !args.root.is_dir() {
        bail!("{} exists and is not a directory", args.root.display());
    }

    let service = tower::service_fn(move |req| {
        transfer_handler(args.root.clone(), req).map(Ok::<_, Infallible>)
    });
    pipeline::Server::new(transport, service)
        .await
        .map_err(|e| anyhow!(e.to_string()))
        .context("handle-transfer server failed")?;

    Ok(())
}

async fn transfer_handler(root: PathBuf, req: TransferRequest) -> TransferResponse {
    debug!(?req, "incoming request");

    let id = req.id;
    let res = match req.kind {
        proto::TransferRequestKind::Check => handle_check(root, req),
        proto::TransferRequestKind::Delta => handle_delta(root, req),
        proto::TransferRequestKind::Contents => handle_contents(root, req).await,
    };
    match res {
        Ok(resp) => resp,
        Err(e) => TransferResponse {
            id,
            kind: TransferResponseKind::CantHandle {
                reason: e.to_string(),
            },
        },
    }
}

fn handle_check(root: PathBuf, req: TransferRequest) -> anyhow::Result<TransferResponse> {
    let path = root.join(req.path);
    match req.file_type {
        FileType::Dir => {
            let kind = handle_check_dir(path).unwrap_or_else(From::from);
            Ok(TransferResponse { id: req.id, kind })
        }
        FileType::File => {
            let transfer = req
                .transfer
                .ok_or_else(|| anyhow!("missing transfer data on check file request"))?;
            let kind = handle_check_file(path, transfer).unwrap_or_else(From::from);
            Ok(TransferResponse { id: req.id, kind })
        }
        FileType::Symlink => todo!(),
    }
}

fn handle_check_dir(path: PathBuf) -> io::Result<TransferResponseKind> {
    Ok(if path.exists() {
        if !path.is_dir() {
            fs::remove_file(&path)?;
            fs::create_dir_all(path)?;
            TransferResponseKind::Created
        } else {
            TransferResponseKind::Exists
        }
    } else {
        fs::create_dir_all(path)?;
        TransferResponseKind::Created
    })
}

fn handle_check_file(path: PathBuf, transfer: Transfer) -> io::Result<TransferResponseKind> {
    debug!(
        "handle_check_file at {} with transfer {:?}",
        path.display(),
        transfer
    );
    if !path.exists() {
        Ok(TransferResponseKind::NeedContents)
    } else {
        let mmap = unsafe { MmapOptions::new().map_copy(&File::open(path)?)? };
        let shasum = shasum_bytes(&mmap);

        if shasum == transfer.shasum {
            Ok(TransferResponseKind::Exists)
        } else {
            // TODO: Reuse buffers
            let mut storage = Vec::new();
            let mut signature = Vec::new();
            let signature_options = SignatureOptions {
                block_size: 4096,
                crypto_hash_size: 8,
            };
            Signature::calculate(&mmap, &mut storage, signature_options).serialize(&mut signature);
            Ok(TransferResponseKind::ExistsDifferent { signature })
        }
    }
}

async fn handle_contents(root: PathBuf, req: TransferRequest) -> anyhow::Result<TransferResponse> {
    if req.file_type != FileType::File {
        bail!("contents request for a non-file");
    }
    let transfer = req
        .transfer
        .ok_or_else(|| anyhow!("transfer data missing for contents request"))?;
    if transfer.kind != TransferKind::Contents {
        bail!("transfer kind is not contents for contents request");
    }

    let path = root.join(req.path);
    let mut f = tokio::fs::File::create(path).await?;

    tokio::io::copy_buf(&mut transfer.data.as_slice(), &mut f).await?;
    // TODO: maybe it is a good idea to calculate shasum, however we would need to hook it in
    // into the AsyncRead

    Ok(TransferResponse {
        id: req.id,
        kind: TransferResponseKind::Created,
    })
}

fn handle_delta(root: PathBuf, req: TransferRequest) -> anyhow::Result<TransferResponse> {
    if req.file_type != FileType::File {
        bail!("delta request for a non-file");
    }
    let transfer = req
        .transfer
        .ok_or_else(|| anyhow!("transfer data missing for contents request"))?;
    if transfer.kind != TransferKind::Delta {
        bail!("transfer kind is not delta for delta request");
    }
    let len = transfer
        .len
        .ok_or_else(|| anyhow!("transfer len is missing for delta transfer"))?;

    let path = root.join(req.path);

    // TODO: Reuse mmap from previous request
    let mmap = unsafe { MmapOptions::new().map_copy(&File::open(&path)?)? };

    let f = File::create(path)?;
    let mut out = WriterWithShasum::new(BufWriter::new(f));

    apply_limited(&mmap, &transfer.data, &mut out, len)?;
    let shasum = out.finalize();

    if shasum == transfer.shasum {
        // apply worked
        Ok(TransferResponse {
            id: req.id,
            kind: TransferResponseKind::Created,
        })
    } else {
        // apply failed => ask for the full contents
        Ok(TransferResponse {
            id: req.id,
            kind: TransferResponseKind::NeedContents,
        })
    }
}
