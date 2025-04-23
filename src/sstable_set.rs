use tokio::io::{BufReader, Result, Error, ErrorKind};

use crate::{sparse_index::{self,SparseIndex}, Config, Manifest};
use crate::version;

#[derive(Debug)]
pub struct SSTable {
    pub index: SparseIndex,
    pub index_path: String,
    pub data_path: String,
}

#[derive(Debug)]
pub struct SSTableSet {
    pub last_sequence: usize,
    pub tables: Vec<SSTable>,
}

impl SSTableSet {
    pub async fn build(config: &Config, manifest: &Manifest) -> Result<SSTableSet> {
        if manifest.version != version::VERSION {
            panic!("MANIFEST version={}, unable to handle it with version={}", manifest.version, version::VERSION);
        }

        let indexes: Vec<_> = manifest.sstables
            .iter()
            .map(|entry| {
                let data_path = entry.data_path.clone();
                let index_path = entry.index_path.clone();

                async move {
                    log::info!("Loading sparse index from: {}...", config.data_dir.join(&index_path).to_str().unwrap());
                    let reader = BufReader::new(tokio::fs::File::open(config.data_dir.join(&index_path)).await?);
                    let index = sparse_index::read_sparse_index(reader).await?;
                    if index.len() == 0 {
                        return Err(Error::new(ErrorKind::InvalidData, "Index can't be empty"))
                    }
                    log::info!("Done!");
                    let data_path = data_path
                        .into_os_string()
                        .into_string()
                        .map_err(|_| tokio::io::Error::new(
                            tokio::io::ErrorKind::InvalidData,
                            "Non-UTF-8 file path in manifest"
                        ))?;
                    let index_path: String = index_path
                        .into_os_string()
                        .into_string()
                        .map_err(|_| tokio::io::Error::new(
                            tokio::io::ErrorKind::InvalidData,
                            "Non-UTF-8 file path in manifest"
                        ))?;
                    Ok(SSTable { index, data_path, index_path })
                }
            })
            .collect();

        let results = futures::future::join_all(indexes).await;
        let tables: Result<Vec<_>> = results.into_iter().collect();

        let sstable_set = SSTableSet { last_sequence: manifest.last_sequence, tables: tables? };
        Ok(sstable_set)
    }
}
