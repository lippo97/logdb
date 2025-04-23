use log;
use sstable_set::{SSTable, SSTableSet};
use std::{collections::BTreeMap, path::Path, u64};
use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Result};

mod config;
mod manifest;
mod sparse_index;
mod sstable_set;
mod version;

pub use config::Config;
pub use manifest::Manifest;
use sparse_index::SparseIndex;

type MemTable = BTreeMap<String, String>;

#[derive(Debug)]
pub struct Database {
    memtable: MemTable,
    sstable_set: SSTableSet,
    config: Config,
}

impl Database {
    pub async fn build(config: config::Config) -> tokio::io::Result<Self> {
        let manifest_path = Self::get_manifest_path(&config.data_dir);
        let manifest_exists = tokio::fs::metadata(&manifest_path).await.is_ok();

        let manifest = if manifest_exists {
            log::info!("Manifest file detected: {}", &manifest_path);
            let contents = tokio::fs::read_to_string(&manifest_path).await?;
            let parsed = toml::from_str::<Manifest>(&contents);

            match parsed {
                Ok(value) => value,
                Err(_) => panic!("Unable to parse MANIFEST"),
            }
        } else {
            if !config.create_if_missing {
                log::error!(
                    "Can't find manifest file: {} (`create_if_missing = false`)",
                    &manifest_path
                );
                return Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::NotFound,
                    format!(
                        "No such file {} (with option `create_if_missing = false`)",
                        &manifest_path
                    ),
                ));
            }

            let manifest = Manifest {
                sstables: Vec::new(),
                last_sequence: 0,
                version: version::VERSION.to_string(),
            };
            let manifest_path = Self::get_manifest_path(&config.data_dir);

            log::info!("Creating manifest file: {}...", &manifest_path);
            manifest::write_manifest(
                &manifest,
                &mut tokio::io::BufWriter::new(tokio::fs::File::create(&manifest_path).await?),
            )
            .await?;
            log::info!("Done.");

            manifest
        };

        log::info!("Using configuration: {:#?}", manifest);
        let sstable_set = SSTableSet::build(&manifest, Some(&config.data_dir)).await?;

        Ok(Self {
            config,
            sstable_set,
            memtable: BTreeMap::new(),
        })
    }

    pub async fn flush(&mut self) -> Result<()> {
        let next_sequence = self.sstable_set.last_sequence + 1;
        let data_path = format!("{:0>5}.db", next_sequence);
        let index_path = format!("{:0>5}.idx", next_sequence);
        let mut data_writer =
            BufWriter::new(tokio::fs::File::create(self.config.data_dir.join(&data_path)).await?);
        let mut index_writer =
            BufWriter::new(tokio::fs::File::create(self.config.data_dir.join(&index_path)).await?);

        let mut index = SparseIndex::new();
        let mut offset: u64 = 0;

        let entries = std::mem::take(&mut self.memtable);
        log::info!(
            "Flushing memtable to {} ({} entries)...",
            data_path,
            entries.len()
        );
        for (i, (key, value)) in entries.into_iter().enumerate() {
            let len = sstable_set::write_record(&mut data_writer, &key, &value).await?;

            if i % self.config.sparse_stride == 0 {
                index.insert(key, offset);
            }

            offset += len;
        }

        log::info!("Writing index to {}...", index_path);
        let sparse_index_fut = sparse_index::write_sparse_index(&index, &mut index_writer);
        let (data_res, index_res) =
            futures::future::join(data_writer.flush(), sparse_index_fut).await;
        data_res?;
        index_res?;
        log::info!("Done.");

        self.sstable_set.tables.insert(
            0,
            SSTable {
                index,
                data_path,
                index_path,
            },
        );
        self.sstable_set.last_sequence = next_sequence;

        let manifest_path = Self::get_manifest_path(&self.config.data_dir);
        log::info!("Writing manifest file: {}...", &manifest_path);
        manifest::write_manifest(
            &Manifest::new(&self.sstable_set),
            &mut tokio::io::BufWriter::new(tokio::fs::File::create(&manifest_path).await?),
        )
        .await?;
        log::info!("Done.");
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let mem = &self.memtable;
        let value = mem.get(key);

        if value.is_some() {
            return Ok(value.cloned());
        }

        for SSTable {
            index, data_path, ..
        } in &self.sstable_set.tables
        {
            let range = sparse_index::bounds(&index, key);
            let mut file =
                BufReader::new(tokio::fs::File::open(&self.config.data_dir.join(data_path)).await?);
            let read = sstable_set::seek_and_read(&mut file, key, range).await?;
            if read.is_some() {
                return Ok(read);
            }
        }
        Ok(None)
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        self.memtable.insert(key, value);
        Ok(())
    }

    pub async fn delete(&mut self, key: &str) -> Result<Option<String>> {
        let mem = &mut self.memtable;
        Ok(mem.remove(key))
    }

    fn get_manifest_path(data_dir: &Path) -> String {
        data_dir
            .join("MANIFEST")
            .into_os_string()
            .into_string()
            .unwrap()
    }
}
