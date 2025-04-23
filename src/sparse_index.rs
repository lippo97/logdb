use std::collections::BTreeMap;

use tokio::io::{AsyncWrite, AsyncWriteExt, AsyncReadExt, Result};

pub type SparseIndex = BTreeMap<String, u64>;

#[derive(Debug)]
pub enum ScanRange {
    Exact { offset: u64 },
    FromBegin { end: u64 },
    Range { start: u64, end: u64 },
    ToEnd { start: u64 }
}

pub fn bounds(index: &SparseIndex, key: &str) -> ScanRange {
    let upper = index.range(key.to_string()..).next();
    let lower = index.range(..=key.to_string()).next_back();

    match (lower, upper) {
        (Some((_, &lower_offset)), Some((_, &upper_offset)))
            if lower_offset == upper_offset => 
                ScanRange::Exact { offset: lower_offset },
        (Some((_, &lower_offset)), Some((_, &upper_offset))) => 
            ScanRange::Range { start: lower_offset, end: upper_offset },
        (Some((_, &lower_offset)), None) =>
            ScanRange::ToEnd { start: lower_offset },
        (None, Some((_, &upper_offset))) =>
            ScanRange::FromBegin { end: upper_offset },
        _ => {
            dbg!((lower, upper));
            panic!("IT SHOULD NOT HAPPEN");
        },
    }
}

/// Writes a sparse index to the given writer.
/// Each entry: [key_len (u16)][key bytes][offset (u64)]
pub async fn write_sparse_index<W: AsyncWrite + Unpin>(
    index: &SparseIndex,
    writer: &mut W,
) -> Result<()> {
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
pub async fn read_sparse_index<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<SparseIndex> {
    let mut index = BTreeMap::new();
    let mut len_buf = [0u8; 2];
    let mut offset_buf = [0u8; 8];

    loop {
        // Read key_len
        if reader.read_exact(&mut len_buf).await.is_err() {
            break; // EOF
        }
        let key_len = u16::from_be_bytes(len_buf) as usize;

        // Read key
        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf).await?;

        // Read offset
        reader.read_exact(&mut offset_buf).await?;
        let offset = u64::from_be_bytes(offset_buf);

        let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in key");
        index.insert(key, offset);
    }

    Ok(index)
}
