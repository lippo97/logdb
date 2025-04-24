use std::io::SeekFrom;
use std::path::Path;

use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, BufReader, Error, ErrorKind, Result,
};

use crate::record::{MemValue, Record};
use crate::sparse_index::ScanRange;
use crate::version;
use crate::{
    Manifest,
    sparse_index::{self, SparseIndex},
};

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
    pub async fn build(manifest: &Manifest, data_dir: Option<&Path>) -> Result<SSTableSet> {
        let data_dir = data_dir.unwrap_or(Path::new("."));
        if manifest.version != version::VERSION {
            panic!(
                "MANIFEST version={}, unable to handle it with version={}",
                manifest.version,
                version::VERSION
            );
        }

        let indexes: Vec<_> = manifest
            .sstables
            .iter()
            .map(|entry| {
                let data_path = entry.data_path.clone();
                let index_path = entry.index_path.clone();

                async move {
                    log::info!(
                        "Loading sparse index from: {}...",
                        data_dir.join(&index_path).to_str().unwrap()
                    );
                    let reader =
                        BufReader::new(tokio::fs::File::open(data_dir.join(&index_path)).await?);
                    let index = sparse_index::read_from(reader).await?;
                    if index.len() == 0 {
                        return Err(Error::new(ErrorKind::InvalidData, "Index can't be empty"));
                    }
                    log::info!("Done!");
                    let data_path = data_path.into_os_string().into_string().map_err(|_| {
                        tokio::io::Error::new(
                            tokio::io::ErrorKind::InvalidData,
                            "Non-UTF-8 file path in manifest",
                        )
                    })?;
                    let index_path: String =
                        index_path.into_os_string().into_string().map_err(|_| {
                            tokio::io::Error::new(
                                tokio::io::ErrorKind::InvalidData,
                                "Non-UTF-8 file path in manifest",
                            )
                        })?;
                    Ok(SSTable {
                        index,
                        data_path,
                        index_path,
                    })
                }
            })
            .collect();

        let results = futures::future::join_all(indexes).await;
        let tables: Result<Vec<_>> = results.into_iter().collect();

        let sstable_set = SSTableSet {
            last_sequence: manifest.last_sequence,
            tables: tables?,
        };
        Ok(sstable_set)
    }
}

pub async fn seek_and_read<R>(
    file: &mut R,
    key: &str,
    scan_range: ScanRange,
) -> Result<Option<MemValue>>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    match scan_range {
        ScanRange::Exact { offset } => {
            let record = read_exact(file, offset).await?;
            if record.key != key {
                panic!(
                    "Exact key read doesn't match expected key: read_key={}",
                    &record.key
                );
            }
            Ok(Some(record.value))
        }
        ScanRange::FromBegin { end } => scan_file_for_key(file, key, None, Some(end)).await,
        ScanRange::ToEnd { start } => scan_file_for_key(file, key, Some(start), None).await,
        ScanRange::Range { start, end } => {
            scan_file_for_key(file, key, Some(start), Some(end)).await
        }
    }
}

async fn read_exact<R>(reader: &mut R, offset: u64) -> tokio::io::Result<Record>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    reader.seek(std::io::SeekFrom::Start(offset)).await?;
    Record::read_from(reader).await
}

async fn scan_file_for_key<R>(
    reader: &mut R,
    key: &str,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<Option<MemValue>>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    assert!(
        start.is_some() || end.is_some(),
        "At least one of `start` or `end` must be provided"
    );

    let mut len_buf = [0u8; 2]; // shared buffer for {key,val}_len
    let mut key_buf = Vec::with_capacity(256);
    let mut type_tag_buf = [0u8; 1];
    let mut offset = start.unwrap_or(0);
    let end_offset = end.unwrap_or(u64::MAX);

    reader.seek(std::io::SeekFrom::Start(offset)).await?;

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

        reader.read_exact(&mut type_tag_buf).await?;

        key_buf.resize(key_len, 0);
        reader.read_exact(&mut key_buf).await?;
        let read_key = String::from_utf8(std::mem::take(&mut key_buf))
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        if read_key == key {
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf).await?;
            let value = MemValue::deserialize(type_tag_buf[0], &val_buf);
            return value.map(|x| Some(x));
        }

        reader.seek(SeekFrom::Current(val_len as i64)).await?;

        offset += (2 + 2 + key_len + val_len) as u64;
    }
}
