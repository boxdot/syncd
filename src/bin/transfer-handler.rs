use std::fmt::Debug;
use std::path::PathBuf;

use anyhow::{anyhow, Context as _};
use argh::FromArgs;
use syncd::proto::{TransferRequest, TransferResponse, TransferResponseKind};
use syncd::{init, proto, transport};
use tokio_tower::pipeline;
use tracing::info;

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

    let service = tower::service_fn(move |req| transfer_handler(args.root.clone(), req));
    pipeline::Server::new(transport, service)
        .await
        .map_err(|e| anyhow!(e.to_string()))
        .context("handle-transfer server failed")?;

    Ok(())
}

async fn transfer_handler(
    _root: PathBuf,
    req: TransferRequest,
) -> anyhow::Result<TransferResponse> {
    info!(?req, "incoming request");
    Ok(TransferResponse {
        id: req.id,
        kind: TransferResponseKind::Exists,
    })
}
