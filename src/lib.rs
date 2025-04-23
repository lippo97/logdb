use log;
use sstable_set::{SSTable, SSTableSet};
use std::{collections::BTreeMap, path::Path, u64};
use tokio::{
    fs::File,
    io::{
        AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
        BufWriter, Error, ErrorKind, Result, SeekFrom,
    },
};

mod config;
mod manifest;
mod sparse_index;
mod sstable_set;
mod version;

pub use config::Config;
pub use manifest::Manifest;
use sparse_index::{ScanRange, SparseIndex};

type MemTable = BTreeMap<String, String>;

#[derive(Debug)]
pub struct Database {
    memtable: MemTable,
    sstable_set: SSTableSet,
    config: Config,
}

impl Database {
    pub async fn build(config: config::Config) -> tokio::io::Result<Database> {
        let manifest_path = Database::get_manifest_path(&config.data_dir);
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
            let manifest_path = Database::get_manifest_path(&config.data_dir);

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
        let sstable_set = SSTableSet::build(&config, &manifest).await?;

        Ok(Database {
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
            let len = write_record(&mut data_writer, &key, &value).await?;

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

        let manifest_path = Database::get_manifest_path(&self.config.data_dir);
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
            let read =
                Database::seek_and_read(&self.config.data_dir.join(data_path), key, range).await?;
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

    async fn seek_and_read(
        file: &Path,
        key: &str,
        scan_range: ScanRange,
    ) -> Result<Option<String>> {
        let mut file = BufReader::new(File::open(file).await?);

        match scan_range {
            ScanRange::Exact { offset } => {
                let (read_key, read_value) = Database::read_exact(&mut file, offset).await?;
                if read_key != key {
                    panic!("Exact key read doesn't match expected key: read_key={read_key}");
                }
                Ok(Some(read_value))
            }
            ScanRange::FromBegin { end } => {
                Database::scan_range(&mut file, key, None, Some(end)).await
            }
            ScanRange::ToEnd { start } => {
                Database::scan_range(&mut file, key, Some(start), None).await
            }
            ScanRange::Range { start, end } => {
                Database::scan_range(&mut file, key, Some(start), Some(end)).await
            }
        }
    }

    async fn read_exact<R>(reader: &mut R, offset: u64) -> tokio::io::Result<(String, String)>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        let mut len_buf = [0u8; 2];

        reader.seek(std::io::SeekFrom::Start(offset)).await?;

        reader.read_exact(&mut len_buf).await?;
        let key_len = u16::from_be_bytes(len_buf) as usize;

        reader.read_exact(&mut len_buf).await?;
        let val_len = u16::from_be_bytes(len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        let mut val_buf = vec![0u8; val_len];

        reader.read_exact(&mut key_buf).await?;
        reader.read_exact(&mut val_buf).await?;

        Ok((
            String::from_utf8(key_buf).unwrap(),
            String::from_utf8(val_buf).unwrap(),
        ))
    }

    async fn scan_range<R>(
        reader: &mut R,
        key: &str,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Option<String>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        assert!(
            start.is_some() || end.is_some(),
            "At least one of `start` or `end` must be provided"
        );

        let mut len_buf = [0u8; 2];
        let mut key_buf = Vec::with_capacity(256);
        let mut offset = start.unwrap_or(0);
        let end_offset = end.unwrap_or(u64::MAX);

        reader
            .seek(std::io::SeekFrom::Start(start.unwrap_or_default()))
            .await?;

        loop {
            if offset > end_offset {
                return Ok(None);
            }

            if let Err(e) = reader.read_exact(&mut len_buf).await {
                if e.kind() == ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(e);
            }

            let key_len = u16::from_be_bytes(len_buf) as usize;

            reader.read_exact(&mut len_buf).await?;
            let val_len = u16::from_be_bytes(len_buf) as usize;

            key_buf.resize(key_len, 0);
            reader.read_exact(&mut key_buf).await?;
            let read_key = String::from_utf8(std::mem::take(&mut key_buf))
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

            if read_key == key {
                let mut val_buf = vec![0u8; val_len];
                reader.read_exact(&mut val_buf).await?;
                let val_str = String::from_utf8(val_buf)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                return Ok(Some(val_str));
            }

            reader.seek(SeekFrom::Current(val_len as i64)).await?;

            offset += (2 + 2 + key_len + val_len) as u64;
        }
    }

    fn get_manifest_path(data_dir: &Path) -> String {
        data_dir
            .join("MANIFEST")
            .into_os_string()
            .into_string()
            .unwrap()
    }
}

async fn write_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    key: &str,
    value: &str,
) -> Result<u64> {
    let key_bytes = key.as_bytes();
    let val_bytes = value.as_bytes();
    let key_len = key_bytes.len() as u16;
    let val_len = val_bytes.len() as u16;

    let mut offset = 0;
    offset += writer.write(&key_len.to_be_bytes()).await? as u64;
    offset += writer.write(&val_len.to_be_bytes()).await? as u64;
    offset += writer.write(key_bytes).await? as u64;
    offset += writer.write(val_bytes).await? as u64;

    Ok(offset)
}
