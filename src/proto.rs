use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct TransferRequest {
    pub id: Uuid,
    pub path: PathBuf,
    pub file_type: FileType,
    pub kind: TransferRequestKind,
    pub transfer: Option<Transfer>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum FileType {
    Dir,
    File,
    Symlink,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum TransferRequestKind {
    Check,
    Delta,
    Contents,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransferResponse {
    pub id: Uuid,
    pub kind: TransferResponseKind,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum TransferResponseKind {
    Exists,
    ExistsDifferent { signature: Vec<u8> },
    Missing,
    NeedContents,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Transfer {
    pub data: Vec<u8>,
    pub kind: TransferKind,
    pub shasum: [u8; 32],
}

#[derive(Debug, Deserialize, Serialize)]
pub enum TransferKind {
    Contents,
    Delta,
    Signature,
}
