use std::path::PathBuf;

#[derive(Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub sparse_stride: usize,
    pub memtable_capacity: usize,
    pub create_if_missing: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            sparse_stride: 50, 
            memtable_capacity: 1000,
            create_if_missing: true,
        }
    }
}

