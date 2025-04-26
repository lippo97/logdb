use std::{collections::BinaryHeap, path::Path};

use tokio::{
    fs::File,
    io::{AsyncWrite, BufReader, Result},
};

use crate::{
    record::{MemValue, Record},
    sparse_index::SparseIndex,
    sstable_set::SSTableSet,
};

#[derive(Debug)]
struct HeapEntry {
    key: String,
    priority: usize,
    value: MemValue,
}

pub async fn compact_sstable_set<W>(
    sstable_set: &mut SSTableSet,
    output: &mut W,
    data_dir: &Path,
    index_stride: usize,
) -> Result<SparseIndex>
where
    W: AsyncWrite + Unpin,
{
    let mut readers = Vec::new();
    let mut heap = BinaryHeap::new();
    let mut index = SparseIndex::new();
    let mut offset = 0u64;
    let mut i = 0;

    let inputs: Vec<_> = sstable_set
        .tables
        .iter()
        .map(|t| data_dir.join(&t.data_path))
        .collect();

    for (i, path) in inputs.iter().enumerate() {
        let file = File::open(path).await?;
        let mut reader = BufReader::new(file);
        if let Ok(record) = Record::read_from(&mut reader).await {
            heap.push(HeapEntry {
                key: record.key,
                value: record.value,
                priority: i,
            });
        }
        readers.push(reader);
    }

    while let Some(entry) = heap.pop() {
        // Peek and check if the next element shares the key with the current one.
        // In case they do, discard it (as `HeapEntries` are sorted by `key`, `priority`),
        // and replace it with the next element from the same log file.
        while let Some(next) = heap.peek() {
            if next.key != entry.key {
                break;
            }
            let next = heap.pop().unwrap();
            // When no record is found the log is consumed.
            if let Ok(record) = Record::read_from(&mut readers[next.priority]).await {
                heap.push(HeapEntry {
                    key: record.key,
                    value: record.value,
                    priority: next.priority,
                });
            }
        }

        if !matches!(entry.value, MemValue::Tombstone) {
            let record = Record {
                key: entry.key,
                value: entry.value,
            };
            // Save offset before writing data
            if i % index_stride == 0 {
                index.insert(record.key.clone(), offset);
            }

            offset += record.write_to(output).await?;
            i += 1;
        }

        // Refill from the file that provided the last inserted key
        let reader = &mut readers[entry.priority];
        if let Ok(record) = Record::read_from(reader).await {
            heap.push(HeapEntry {
                key: record.key,
                value: record.value,
                priority: entry.priority,
            });
        }
    }

    Ok(index)
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key) && self.priority.eq(&other.priority)
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .reverse()
            .then_with(|| self.priority.cmp(&other.priority).reverse())
    }
}
