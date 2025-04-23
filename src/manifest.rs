use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::{AsyncWrite, AsyncWriteExt, Result};

use crate::sstable_set::SSTableSet;
use crate::version;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: String,
    pub last_sequence: usize,
    pub sstables: Vec<SSTableEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableEntry {
    pub data_path: PathBuf,
    pub index_path: PathBuf,
}

impl Manifest {
    pub fn new(sstable_set: &SSTableSet) -> Manifest {
        let sstables = sstable_set
            .tables
            .iter()
            .map(|table| SSTableEntry {
                data_path: table.data_path.clone().into(),
                index_path: table.index_path.clone().into(),
            })
            .collect();
        Self {
            version: version::VERSION.to_owned(),
            sstables,
            last_sequence: sstable_set.last_sequence,
        }
    }
}

pub async fn write_manifest<W: AsyncWrite + Unpin>(
    manifest: &Manifest,
    writer: &mut W,
) -> Result<()> {
    let serialized = toml::to_string(&manifest).map_err(|_| {
        tokio::io::Error::new(
            tokio::io::ErrorKind::InvalidData,
            format!("Unable to serialize {:?}", manifest),
        )
    })?;

    writer.write_all(serialized.as_bytes()).await?;
    writer.flush().await
}
