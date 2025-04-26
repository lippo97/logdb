use futures::future::try_join_all;
use log;
use memtable::MemTable;
use record::MemValue;
use sstable_set::{SSTable, SSTableSet};
use std::{collections::BTreeMap, path::Path};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufReader, BufWriter, Error, Result},
    join,
};

mod compact;
mod config;
mod manifest;
mod memtable;
mod record;
mod sparse_index;
mod sstable_set;
mod version;

pub use config::Config;
pub use manifest::Manifest;
pub use record::Value;

#[derive(Debug)]
pub struct Database {
    memtable: MemTable,
    sstable_set: SSTableSet,
    config: Config,
}

impl Database {
    pub async fn build(config: Config) -> Result<Self> {
        let manifest =
            Self::get_or_create_manifest(&config.data_dir, config.create_if_missing).await?;
        log::info!("Using configuration:\n{:#?}", manifest);
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
            BufWriter::new(File::create(self.config.data_dir.join(&data_path)).await?);
        let mut index_writer =
            BufWriter::new(File::create(self.config.data_dir.join(&index_path)).await?);

        log::info!(
            "Flushing memtable to {} ({} entries)...",
            data_path,
            self.memtable.len(),
        );
        let index = memtable::flush_to(
            &mut self.memtable,
            &mut data_writer,
            self.config.sparse_stride,
        )
        .await?;

        log::info!("Writing index to {}...", index_path);
        sparse_index::write_to(&index, &mut index_writer).await?;
        let (data_res, index_res) =
            futures::future::join(data_writer.flush(), index_writer.flush()).await;
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
            &mut BufWriter::new(File::create(&manifest_path).await?),
        )
        .await?;
        log::info!("Done.");
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        if let Some(inner) = self.memtable.get(key) {
            return Ok(inner.clone().to_value());
        }

        for SSTable {
            index, data_path, ..
        } in &self.sstable_set.tables
        {
            let range = sparse_index::bounds(&index, key);
            let mut file = BufReader::new(File::open(&self.config.data_dir.join(data_path)).await?);

            if let Some(inner) = sstable_set::seek_and_read(&mut file, key, range).await? {
                return Ok(inner.to_value());
            }
        }
        Ok(None)
    }

    pub async fn set(&mut self, key: String, value: Value) -> Result<()> {
        self.memtable.insert(key, MemValue::Value(value));
        Ok(())
    }

    pub async fn delete(&mut self, key: String) -> Result<()> {
        self.memtable.insert(key, MemValue::Tombstone);
        Ok(())
    }

    pub async fn compact(&mut self) -> Result<()> {
        if self.sstable_set.tables.len() < 2 {
            return Ok(());
        }

        let data_path_part = self.config.data_dir.join("compact.db.part");
        let idx_path_part = self.config.data_dir.join("compact.idx.part");
        let final_data_path = self.config.data_dir.join("00001.db");
        let final_idx_path = self.config.data_dir.join("00001.idx");

        let data_files: Vec<_> = self
            .sstable_set
            .tables
            .iter()
            .map(|x| self.config.data_dir.join(&x.data_path))
            .collect();
        let index_files: Vec<_> = self
            .sstable_set
            .tables
            .iter()
            .map(|x| self.config.data_dir.join(&x.index_path))
            .collect();
        let mut output = File::create(&data_path_part).await?;
        let mut output_idx = File::create(&idx_path_part).await?;

        log::info!("Starting log compaction.");
        log::info!("Input log files: {:#?}", data_files,);
        log::info!("Output log file: {}", data_path_part.to_str().unwrap());
        let index = compact::compact_sstable_set(
            &mut self.sstable_set,
            &mut output,
            &self.config.data_dir,
            self.config.sparse_stride,
        )
        .await?;
        sparse_index::write_to(&index, &mut output_idx).await?;
        log::info!("Finished log compaction.");

        log::info!("Deleting input files: {:?}", data_files);
        let _ = try_join_all(
            data_files
                .into_iter()
                .chain(index_files)
                .map(tokio::fs::remove_file),
        )
        .await?;
        let _ = join!(
            tokio::fs::rename(data_path_part, final_data_path),
            tokio::fs::rename(idx_path_part, final_idx_path),
        );

        self.sstable_set.tables.clear();
        self.sstable_set.tables.push(SSTable {
            index,
            index_path: "00001.idx".to_string(),
            data_path: "00001.db".to_string(),
        });
        self.sstable_set.last_sequence = 1;

        let manifest_path = Database::get_manifest_path(&self.config.data_dir);
        log::info!("Updating manifest file: {}...", &manifest_path);
        manifest::write_manifest(
            &Manifest::new(&self.sstable_set),
            &mut BufWriter::new(File::create(&manifest_path).await?),
        )
        .await
    }

    pub async fn dump(&self) -> Result<()> {
        log::info!("Dumping memtable:\n{:#?}", self.memtable);
        Ok(())
    }

    async fn get_or_create_manifest(data_dir: &Path, create_if_missing: bool) -> Result<Manifest> {
        let manifest_path = Self::get_manifest_path(data_dir);
        let manifest_exists = tokio::fs::metadata(&manifest_path).await.is_ok();

        if manifest_exists {
            log::info!("Manifest file detected: {}", &manifest_path);
            let contents = tokio::fs::read_to_string(&manifest_path).await?;
            return toml::from_str::<Manifest>(&contents).map_err(|_| {
                Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "Unable to parse MANIFEST file",
                )
            });
        }
        if !create_if_missing {
            return Err(Error::new(
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
        let manifest_path = Self::get_manifest_path(data_dir);

        log::info!("Creating manifest file: {}...", &manifest_path);
        manifest::write_manifest(
            &manifest,
            &mut BufWriter::new(File::create(&manifest_path).await?),
        )
        .await?;
        log::info!("Done.");

        Ok(manifest)
    }

    fn get_manifest_path(data_dir: &Path) -> String {
        data_dir
            .join("MANIFEST")
            .into_os_string()
            .into_string()
            .unwrap()
    }
}
