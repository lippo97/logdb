use std::collections::BTreeMap;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, Result};

pub type SparseIndex = BTreeMap<String, u64>;

#[derive(Debug)]
pub enum ScanRange {
    Exact { offset: u64 },
    FromBegin { end: u64 },
    Range { start: u64, end: u64 },
    ToEnd { start: u64 },
}

/// Inspects a sparse index for a key.
pub fn bounds(index: &SparseIndex, key: &str) -> ScanRange {
    let upper = index.range(key.to_string()..).next();
    let lower = index.range(..=key.to_string()).next_back();

    match (lower, upper) {
        (Some((_, &lower_offset)), Some((_, &upper_offset))) if lower_offset == upper_offset => {
            ScanRange::Exact {
                offset: lower_offset,
            }
        }
        (Some((_, &lower_offset)), Some((_, &upper_offset))) => ScanRange::Range {
            start: lower_offset,
            end: upper_offset,
        },
        (Some((_, &lower_offset)), None) => ScanRange::ToEnd {
            start: lower_offset,
        },
        (None, Some((_, &upper_offset))) => ScanRange::FromBegin { end: upper_offset },
        _ => panic!("Illegal state: no `upper` nor `lower` bound found."),
    }
}

/// Writes a sparse index to the given writer.
/// Each entry: [key_len (u16)][key bytes][offset (u64)]
pub async fn write_to<W>(index: &SparseIndex, writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    for (key, &offset) in index {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u16;

        writer.write_all(&key_len.to_be_bytes()).await?;
        writer.write_all(key_bytes).await?;
        writer.write_all(&offset.to_be_bytes()).await?;
    }
    writer.flush().await?;
    Ok(())
}

/// Reads a sparse index from the given reader.
/// Each entry: [key_len (u16)][key bytes][offset (u64)]
pub async fn read_from<R>(mut reader: R) -> Result<SparseIndex>
where
    R: AsyncReadExt + Unpin,
{
    let mut index = BTreeMap::new();
    let mut len_buf = [0u8; 2];
    let mut offset_buf = [0u8; 8];

    loop {
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let key_len = u16::from_be_bytes(len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf).await?;

        reader.read_exact(&mut offset_buf).await?;
        let offset = u64::from_be_bytes(offset_buf);

        let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in key");
        index.insert(key, offset);
    }

    Ok(index)
}
