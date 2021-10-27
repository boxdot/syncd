use std::path::PathBuf;
use std::{fs, io};

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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum FileType {
    Dir,
    File,
    Symlink,
}

impl FileType {
    pub fn from_fs(ft: fs::FileType) -> Option<Self> {
        if ft.is_dir() {
            Some(Self::Dir)
        } else if ft.is_file() {
            Some(Self::File)
        } else if ft.is_symlink() {
            Some(Self::Symlink)
        } else {
            None
        }
    }
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
    Created,
    Exists,
    ExistsDifferent { signature: Vec<u8> },
    NeedContents,
    CantHandle { reason: String },
}

impl From<io::Error> for TransferResponseKind {
    fn from(e: io::Error) -> Self {
        Self::CantHandle {
            reason: e.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Transfer {
    pub kind: TransferKind,
    pub data: Vec<u8>,
    pub shasum: [u8; 32],
    pub len: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum TransferKind {
    Empty,
    Contents,
    Delta,
    Signature,
}
