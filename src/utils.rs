use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::{io, path::Path};

use crate::bloom_filter::BloomFilter;

pub const DATA_DIR: &str = "data";
pub const MANIFEST: &str = "MANIFEST";
pub const MANIFEST_TMP: &str = "MANIFEST.tmp";
pub const SST_FILE_PREFIX: &str = "sst";
pub const SPARSE_INDEX_SUFFIX: &str = "idx";
pub const WAL: &str = "wal.db";

pub fn is_deleted(value: &Option<String>) -> bool {
    return value.is_none();
}

pub fn remove_file_extension(file_name: &str) -> &str {
    let dot_idx = file_name.find(".").unwrap();
    return &file_name[0..dot_idx];
}

#[derive(Debug)]
pub struct HeapEntry {
    pub key: String,
    pub value: Option<String>,
    pub sst_table_pos: u64,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.key != other.key {
            other.key.cmp(&self.key)
        } else {
            self.sst_table_pos.cmp(&other.sst_table_pos)
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sst_table_pos == other.sst_table_pos
    }
}

impl Eq for HeapEntry {}

#[derive(Serialize, Deserialize, Debug)]
pub struct SSTableEntry {
    pub key: String,
    pub value: Option<String>,
}

pub struct SSTIterator {
    pub reader: io::BufReader<File>,
    pub buf: String,
}

impl SSTIterator {
    pub fn open(path: &Path) -> Self {
        return Self::open_at(path, 0);
    }

    pub fn open_at(path: &Path, offset: u64) -> Self {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!("Failed to open file {:?}: {}", path, err),
        };
        file.seek(io::SeekFrom::Start(offset)).unwrap();
        return Self {
            reader: BufReader::new(file),
            buf: String::new(),
        };
    }
}

impl Iterator for SSTIterator {
    type Item = SSTableEntry;
    fn next(&mut self) -> Option<SSTableEntry> {
        self.buf.clear();
        let bytes = self.reader.read_line(&mut self.buf).ok()?;
        if bytes == 0 {
            return None;
        }
        return serde_json::from_str(&self.buf).ok();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WALOp {
    PUT,
    DELETE,
}

#[derive(Debug)]
pub struct SSTMetadata {
    pub file_name: String,
    pub index: SparseIndex,
    pub bloom_filter: BloomFilter,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WALEntry {
    pub operation: WALOp,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct SparseIndexEntry {
    pub key: String,
    pub file_offset: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct SparseIndex {
    pub entries: Vec<SparseIndexEntry>,
}
