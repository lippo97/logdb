use std::collections::BTreeMap;

use tokio::io::{AsyncWrite, Result};

use crate::{record::{MemValue, Record}, sparse_index::SparseIndex};

pub type MemTable = BTreeMap<String, MemValue>;

/// Serializes the current contents of the memtable to the given writer.
///
/// This function consumes all key-value pairs in the provided `memtable` and
/// writes them to the `writer` in a compact binary format. As it writes,
/// it also constructs a `SparseIndex` that maps a subset of keys to their
/// corresponding byte offsets in the output, enabling efficient lookup.
///
/// The `index_stride` parameter controls the sparsity of the index:
/// every `index_stride`-th record will be indexed.
///
/// # Arguments
///
/// * `memtable` - The in-memory table of records to flush. Will be emptied after the operation.
/// * `writer` - The output stream to which the records are written.
/// * `index_stride` - How often to index a record (e.g., 1 = every record, 4 = every 4th record).
///
/// # Returns
///
/// A `SparseIndex` containing the offset of every `index_stride`-th record written.
///
/// # Errors
///
/// Returns an error if writing to the output stream fails.
pub async fn flush_to<W: AsyncWrite + Unpin>(memtable: &mut MemTable, writer: &mut W, index_stride: usize) -> Result<SparseIndex> {
    let mut index = SparseIndex::new();
    let mut offset: u64 = 0;

    let entries = std::mem::take(memtable);
    for (i, (key, value)) in entries.into_iter().enumerate() {
        let record = Record { key, value };
        let len = record.write_to(writer).await?;

        if i % index_stride == 0 {
            index.insert(record.key, offset);
        }

        offset += len;
    }
    Ok(index)
}
