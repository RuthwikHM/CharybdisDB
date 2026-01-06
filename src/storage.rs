use rkyv::rancor::Error;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, from_bytes};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek};
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::{
    fs::{self, File, create_dir},
    io::{self, BufRead, Write},
    path::Path,
    sync::{Arc, atomic::AtomicU32},
};
use tokio::sync::RwLock;

const DATA_DIR: &str = "data";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const SST_FILE_PREFIX: &str = "sst";
const SPARSE_INDEX_SUFFIX: &str = "idx";
const WAL: &str = "wal.db";
const MEMTABLE_LIMIT: usize = 2000;
const COMPACTION_TRIGGER: u32 = 10000;
const WAL_FSYNC_EVERY: u8 = 100;

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    next_sst_id: Arc<AtomicU32>,
    num_updates: Arc<AtomicU32>,
    manifest: Arc<RwLock<Vec<SSTMetadata>>>,
    wal_fd: Arc<Mutex<File>>,
    wal_write_counter: Arc<AtomicU8>,
}

impl KVStore {
    pub async fn new() -> Self {
        let data_dir = Path::new(DATA_DIR);
        if !data_dir.exists() {
            create_dir(data_dir).unwrap();
        }

        let wal_fd = match OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(data_dir.join(WAL))
        {
            Ok(wal_fd) => wal_fd,
            Err(err) => {
                panic!("Unable to open WAL : {}", err)
            }
        };

        let kvstore = Self {
            store: Arc::new(RwLock::new(BTreeMap::new())),
            next_sst_id: Arc::new(AtomicU32::new(0)),
            num_updates: Arc::new(AtomicU32::new(0)),
            manifest: Arc::new(RwLock::new(Vec::<SSTMetadata>::new())),
            wal_fd: Arc::new(Mutex::new(wal_fd)),
            wal_write_counter: Arc::new(AtomicU8::new(0)),
        };
        let data_dir = Path::new(DATA_DIR);
        if !data_dir.exists() {
            match create_dir(data_dir) {
                Ok(_) => {
                    println!("Created data directory:{}", DATA_DIR);
                    kvstore
                        .next_sst_id
                        .store(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(err) => {
                    panic!("Failed to create data directory: {}", err)
                }
            };
        }
        kvstore.manage_manifest(data_dir).await;

        kvstore.replay_wal().await;
        return kvstore;
    }

    pub async fn put(&self, key: String, value: String) {
        let wal_entry = WALEntry {
            operation: WALOp::PUT,
            key: key.clone(),
            value: Some(value.clone()),
        };
        self.add_wal_entry(wal_entry);

        let mut store = self.store.write().await;
        store.insert(key, Some(value));

        if store.len() == MEMTABLE_LIMIT {
            let mut frozen = std::mem::take(&mut *store);
            self.write_memtable_to_sst(&mut frozen).await;
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.num_updates.load(std::sync::atomic::Ordering::Relaxed) == COMPACTION_TRIGGER {
            drop(store);
            self.compact_sst().await;
            self.num_updates
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub async fn delete(&self, key: String) {
        let wal_entry = WALEntry {
            operation: WALOp::DELETE,
            key: key.clone(),
            value: None,
        };
        self.add_wal_entry(wal_entry);

        let mut store = self.store.write().await;
        store.insert(key, None);

        if store.len() == MEMTABLE_LIMIT {
            let mut frozen = std::mem::take(&mut *store);
            self.write_memtable_to_sst(&mut frozen).await;
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.num_updates.load(std::sync::atomic::Ordering::Relaxed) == COMPACTION_TRIGGER {
            drop(store);
            self.compact_sst().await;
            self.num_updates
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub async fn get(&self, key: &str) -> String {
        let store = self.store.read().await;
        match store.get(key) {
            Some(potential_value) => {
                match potential_value {
                    Some(value) => return value.clone(),
                    // Tombstone i.e. deleted value
                    None => return "".to_string(),
                };
            }
            None => {
                drop(store);
                let comp_key = key;
                let sst_files_metadata = self.manifest.read().await;

                for sst_file_meta in sst_files_metadata.iter().rev() {
                    let sst_file_path = Path::new(DATA_DIR).join(&sst_file_meta.file_name);

                    let offset = find_sparse_idx_offset(comp_key, sst_file_meta);

                    for entry in SSTIterator::open_at(&sst_file_path, offset) {
                        match entry.key.as_str().cmp(comp_key) {
                            Ordering::Less => continue,
                            Ordering::Equal => {
                                return match &entry.value {
                                    Some(value) => value.clone(),
                                    None => {
                                        // Tombstone i.e. deleted value
                                        "".to_string()
                                    }
                                };
                            }
                            Ordering::Greater => break,
                        }
                    }
                }
                return "".to_string();
            }
        }
    }

    async fn compact_sst(&self) {
        println!("Compacting SSTs");
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        let manifest = self.manifest.read().await;
        let old_sst_file_paths: Vec<String> = manifest
            .iter()
            .map(|metadata| metadata.file_name.clone())
            .collect();
        // Collect old sst file names and drop read guard
        drop(manifest);
        let mut sst_itrs: Vec<SSTIterator> = old_sst_file_paths
            .iter()
            .map(|sst_file| {
                return SSTIterator::open(Path::new(DATA_DIR).join(&sst_file).as_path());
            })
            .collect();

        for (i, itr) in sst_itrs.iter_mut().enumerate() {
            if let Some(sst_entry) = itr.next() {
                let heap_entry = HeapEntry {
                    key: sst_entry.key,
                    value: sst_entry.value,
                    sst_table_pos: i as u64,
                };
                heap.push(heap_entry);
            }
        }

        let mut new_sst_table: Vec<SSTableEntry> = Vec::with_capacity(MEMTABLE_LIMIT);
        let mut new_sst_files_metadata: Vec<SSTMetadata> = Vec::new();

        while let Some(top) = heap.pop() {
            let key = top.key.clone();
            let mut drained = vec![top];

            while let Some(head) = heap.peek() {
                if head.key == key {
                    drained.push(heap.pop().unwrap());
                } else {
                    break;
                }
            }

            let newest = &drained[0];
            if !is_deleted(&newest.value) {
                new_sst_table.push(SSTableEntry {
                    key: newest.key.clone(),
                    value: newest.value.clone(),
                });

                if new_sst_table.len() == MEMTABLE_LIMIT {
                    // Write all entries to an SST file
                    new_sst_files_metadata.push(self.write_sst(&new_sst_table));
                    new_sst_table.clear();
                }
            }

            for entry in drained {
                let itr = &mut sst_itrs[entry.sst_table_pos as usize];
                if let Some(sst_entry) = itr.next() {
                    let heap_entry = HeapEntry {
                        key: sst_entry.key.to_string(),
                        value: sst_entry.value,
                        sst_table_pos: entry.sst_table_pos,
                    };
                    heap.push(heap_entry);
                }
            }
        }

        if new_sst_table.len() != 0 {
            new_sst_files_metadata.push(self.write_sst(&new_sst_table));
            new_sst_table.clear();
        }

        let mut manifest_entries = self.manifest.write().await;
        *manifest_entries = new_sst_files_metadata;

        self.update_manifest_atomic(
            manifest_entries
                .iter()
                .map(|metadata| &metadata.file_name)
                .collect(),
        );

        old_sst_file_paths.iter().for_each(|sst_file| {
            match fs::remove_file(Path::new(DATA_DIR).join(&sst_file)) {
                Ok(_) => println!("Removed old sst file {}", sst_file),
                Err(err) => panic!("Unable to remove old sst file {}: {}", sst_file, err),
            };
            let sst_idx_name = format!(
                "{}.{}",
                remove_file_extension(&sst_file),
                SPARSE_INDEX_SUFFIX
            );
            match fs::remove_file(Path::new(DATA_DIR).join(&sst_idx_name)) {
                Ok(_) => println!("Removed old sst index file {}", &sst_idx_name),
                Err(err) => panic!("Unable to remove old sst index file {}: {}", sst_file, err),
            };
        });
        println!("Compaction complete")
    }

    async fn write_memtable_to_sst(&self, store: &mut BTreeMap<String, Option<String>>) {
        let mut ss_table: Vec<SSTableEntry> = Vec::new();
        ss_table.reserve(store.len());
        for (key, opt_val) in store.iter() {
            match opt_val {
                Some(value) => {
                    ss_table.push(SSTableEntry {
                        key: key.to_string(),
                        value: Some(value.to_string()),
                    });
                }
                None => {
                    ss_table.push(SSTableEntry {
                        key: key.to_string(),
                        value: None,
                    });
                }
            };
        }

        let sst_files_metadata = vec![self.write_sst(&ss_table)];

        let mut manifest_entries = self.manifest.write().await;
        manifest_entries.extend(sst_files_metadata);

        self.update_manifest_atomic(
            manifest_entries
                .iter()
                .map(|metadata| &metadata.file_name)
                .collect(),
        );

        //Clear MemTable
        store.clear();

        // Truncate WAL after SSTs are persisted and Manifest is updated
        self.truncate_wal();
    }

    fn write_sst(&self, ss_table: &Vec<SSTableEntry>) -> SSTMetadata {
        let current_sst_id = self
            .next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sst_file_name = format!("{}-{}.json", SST_FILE_PREFIX, current_sst_id);
        let sst_file_path = Path::new(DATA_DIR).join(&sst_file_name);
        // let json_string = match serde_json::to_string(&ss_table) {
        //     Ok(json_string) => json_string,
        //     Err(err) => panic!("Couldnt parse to SSTable:{}", err),
        // };
        let sst_file_descriptor = match File::create(sst_file_path) {
            Ok(file) => file,
            Err(err) => panic!("Unable to open SSTable file {}: {}", sst_file_name, err),
        };

        let mut index = SparseIndex { entries: vec![] };
        let mut entry_count = 0;
        let mut offset: u64 = 0;

        let mut writer = BufWriter::new(sst_file_descriptor);

        for sst_entry in ss_table {
            if entry_count % 64 == 0 {
                index.entries.push(SparseIndexEntry {
                    key: sst_entry.key.to_string(),
                    file_offset: offset,
                });
            }

            let line = serde_json::to_string(sst_entry).unwrap();
            offset += line.len() as u64 + 1;

            writer.write_all(format!("{}\n", line).as_bytes()).unwrap();

            entry_count += 1;
        }

        // Fsync SSTable
        writer.flush().unwrap();
        writer.get_ref().sync_all().unwrap();

        // Persist the index
        let idx_file_name = format!(
            "{}-{}.{}",
            SST_FILE_PREFIX, current_sst_id, SPARSE_INDEX_SUFFIX
        );
        let idx_path = Path::new(DATA_DIR).join(idx_file_name);
        let mut idx_fd = File::create(idx_path).unwrap();

        let bytes = rkyv::to_bytes::<Error>(&index).unwrap();
        idx_fd.write_all(&bytes).unwrap();
        idx_fd.sync_all().unwrap();

        File::open(Path::new(DATA_DIR)).unwrap().sync_all().unwrap();
        let sst_file_metadata = SSTMetadata {
            file_name: sst_file_name,
            index,
        };
        return sst_file_metadata;
    }

    fn add_wal_entry(&self, wal_entry: WALEntry) {
        let mut wal_fd = self.wal_fd.lock().unwrap();
        let wal_entry_string = serde_json::to_string(&wal_entry).unwrap() + "\n";
        match wal_fd.write_all(wal_entry_string.as_bytes()) {
            Ok(_) => {
                let old_val = self
                    .wal_write_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if old_val + 1 == WAL_FSYNC_EVERY {
                    wal_fd.sync_data().unwrap();
                    self.wal_write_counter
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Err(err) => {
                panic!("Unable to write WAL entry {}: {}", wal_entry_string, err)
            }
        };
    }

    fn update_manifest_atomic(&self, sst_files: Vec<&String>) {
        let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
        let manifest_tmp_file_path = Path::new(DATA_DIR).join(MANIFEST_TMP);
        let manifest_tmp_fd = File::create(&manifest_tmp_file_path).unwrap();
        let mut writer = io::BufWriter::new(&manifest_tmp_fd);

        for sst_file in sst_files {
            let _ = writer.write(format!("{}\n", &sst_file).as_bytes()).unwrap();
            _ = writer.flush().unwrap();
        }

        _ = manifest_tmp_fd.sync_all().unwrap();

        _ = fs::rename(&manifest_tmp_file_path, &manifest_file_path).unwrap();

        _ = File::open(&manifest_file_path).unwrap().sync_all().unwrap();
        _ = File::open(&manifest_file_path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    fn truncate_wal(&self) {
        let wal_fd = self.wal_fd.lock().unwrap();
        wal_fd.set_len(0).unwrap();
        wal_fd.sync_all().unwrap();
    }

    async fn replay_wal(&self) {
        let mut store = self.store.write().await;
        let mut wal_fd = self.wal_fd.lock().unwrap();
        wal_fd.seek(io::SeekFrom::Start(0)).unwrap();
        let reader = BufReader::new(&*wal_fd);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };

            let wal_entry: WALEntry = match serde_json::from_str(&line) {
                Ok(wal_entry) => wal_entry,
                Err(err) => {
                    println!("Warning couldnt apply WAL entry {}: {}", line, err);
                    continue;
                }
            };
            store.insert(wal_entry.key, wal_entry.value);
        }

        // Truncate WAL after replaying
        wal_fd.set_len(0).unwrap();
        wal_fd.sync_all().unwrap();
    }

    async fn manage_manifest(&self, data_dir: &Path) {
        let manifest_file = data_dir.join(MANIFEST);
        match manifest_file.exists() {
            false => {
                self.next_sst_id
                    .store(1, std::sync::atomic::Ordering::Relaxed);
                let manifest_file_path = manifest_file.clone();
                match File::create(manifest_file) {
                    Ok(_) => {
                        println!("Created manifest file:{}", manifest_file_path.display());
                    }
                    Err(err) => {
                        panic!(
                            "Failed to create manifest file {}: {}",
                            manifest_file_path.display(),
                            err
                        );
                    }
                }
            }
            true => {
                let mut sst_files: HashSet<String> = fs::read_dir(&data_dir)
                    .unwrap()
                    .map(|file| file.unwrap().path().display().to_string())
                    .filter(|file_path| {
                        file_path.starts_with(format!("{}/{}", DATA_DIR, SST_FILE_PREFIX).as_str())
                    })
                    .collect();
                let mut manifest_entries = self.manifest.write().await;
                manifest_entries.extend(fs::read_to_string(manifest_file).unwrap().lines().map(
                    |entry| {
                        let sst_file = remove_file_extension(entry);
                        let index_file_name = format!("{}.{}", sst_file, SPARSE_INDEX_SUFFIX);
                        let mut index_fd =
                            File::open(Path::new(DATA_DIR).join(index_file_name)).unwrap();
                        let mut index_content_bytes: AlignedVec<16> = AlignedVec::new();
                        index_content_bytes
                            .extend_from_reader(&mut index_fd)
                            .unwrap();

                        let index = from_bytes::<SparseIndex, Error>(&index_content_bytes).unwrap();
                        let sst_metadata = SSTMetadata {
                            file_name: entry.to_string(),
                            index: index,
                        };
                        return sst_metadata;
                    },
                ));
                manifest_entries.iter().for_each(|sst_file_meta| {
                    sst_files.remove(
                        &data_dir
                            .join(&sst_file_meta.file_name)
                            .display()
                            .to_string(),
                    );
                });

                sst_files.iter().for_each(|sst_file| {
                    match fs::remove_file(sst_file) {
                        Ok(_) => {}
                        Err(err) => {
                            panic!("Unable to remove dangling SST file {}: {}", sst_file, err)
                        }
                    };
                });
                let next_sst_id: u32 = match manifest_entries.last() {
                    Some(last_file_meta) => {
                        let last_line = &last_file_meta.file_name;
                        let start = last_line.rfind("-").unwrap();
                        let end = last_line.rfind(".").unwrap();
                        match last_line.get(start + 1..end) {
                            Some(value) => value.parse::<u32>().unwrap() + 1,
                            None => 1,
                        }
                    }
                    None => 1,
                };
                // println!(
                //     "Dir:{:?}\nManifest:{:?}\n Last:{}",
                //     sst_files, manifest_entries, last_line
                // );

                self.next_sst_id
                    .store(next_sst_id, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

fn find_sparse_idx_offset(comp_key: &str, sst_file_meta: &SSTMetadata) -> u64 {
    let entries = &sst_file_meta.index.entries;
    let index_size: usize = entries.len();
    let mut l: usize = 0;
    let mut r: usize = index_size;
    while l < r {
        let m = (l + r) / 2;
        if entries[m].key.as_str() <= comp_key {
            l = m + 1;
        } else {
            r = m;
        }
    }
    if l == 0 {
        return 0;
    } else {
        return entries[l - 1].file_offset;
    }
}

fn is_deleted(value: &Option<String>) -> bool {
    return value.is_none();
}

fn remove_file_extension(file_name: &str) -> &str {
    let dot_idx = file_name.find(".").unwrap();
    return &file_name[0..dot_idx];
}

#[derive(Debug)]
struct HeapEntry {
    key: String,
    value: Option<String>,
    sst_table_pos: u64,
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
struct SSTableEntry {
    key: String,
    value: Option<String>,
}

struct SSTIterator {
    reader: io::BufReader<File>,
    buf: String,
}

impl SSTIterator {
    fn open(path: &Path) -> Self {
        return Self::open_at(path, 0);
    }

    fn open_at(path: &Path, offset: u64) -> Self {
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
enum WALOp {
    PUT,
    DELETE,
}

#[derive(Debug)]
struct SSTMetadata {
    file_name: String,
    index: SparseIndex,
}

#[derive(Serialize, Deserialize, Debug)]
struct WALEntry {
    operation: WALOp,
    key: String,
    value: Option<String>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
struct SparseIndexEntry {
    pub key: String,
    pub file_offset: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
struct SparseIndex {
    pub entries: Vec<SparseIndexEntry>,
}
