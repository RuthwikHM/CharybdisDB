use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter};
use std::sync::atomic::AtomicI16;
use std::{
    fs::{self, File, create_dir},
    io::{self, BufRead, Write},
    path::Path,
    sync::{Arc, atomic::AtomicU32},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

const DATA_DIR: &str = "data";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const SST_FILE_PREFIX: &str = "sst";
const WAL: &str = "wal.db";

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    next_sst_id: Arc<AtomicU32>,
    num_updates: Arc<AtomicU32>,
    manifest: Arc<RwLock<Vec<String>>>,
    wal_sync_counter: Arc<AtomicI16>,
}

impl KVStore {
    pub async fn new() -> Self {
        let kvstore = Self {
            store: Arc::new(RwLock::new(BTreeMap::new())),
            next_sst_id: Arc::new(AtomicU32::new(0)),
            num_updates: Arc::new(AtomicU32::new(0)),
            manifest: Arc::new(RwLock::new(Vec::<String>::new())),
            wal_sync_counter: Arc::new(AtomicI16::new(0)),
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
        manage_manifest(&kvstore, data_dir).await;

        replay_wal(&kvstore, data_dir).await;
        return kvstore;
    }

    pub async fn put(&self, key: String, value: String) {
        let mut store = self.store.write().await;

        let wal_entry = WALEntry {
            operation: "put".to_string(),
            key: key.clone(),
            value: Some(value.clone()),
        };
        self.add_wal_entry(wal_entry);

        store.insert(key, Some(value));

        if store.len() == 2000 {
            self.write_memtable_to_sst(&mut store).await;
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.num_updates.load(std::sync::atomic::Ordering::Relaxed) == 10000 {
            drop(store);
            self.compact_sst().await;
            self.num_updates
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub async fn delete(&self, key: String) {
        let mut store = self.store.write().await;

        let wal_entry = WALEntry {
            operation: "delete".to_string(),
            key: key.clone(),
            value: None,
        };
        self.add_wal_entry(wal_entry);

        store.insert(key, None);

        if store.len() == 2000 {
            self.write_memtable_to_sst(&mut store).await;
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.num_updates.load(std::sync::atomic::Ordering::Relaxed) == 10000 {
            drop(store);
            self.compact_sst().await;
            self.num_updates
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub async fn get(&self, key: &String) -> String {
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
                let comp_key = key.to_string();
                let sst_files = self.manifest.read().await;

                for sst_file in sst_files.iter().rev() {
                    let sst_file_path = Path::new(DATA_DIR).join(sst_file);
                    for entry in SSTIterator::open(&sst_file_path) {
                        match entry.key.cmp(&comp_key) {
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
        let sst_file_paths = self.manifest.read().await.clone();
        let mut sst_itrs: Vec<SSTIterator> = sst_file_paths
            .iter()
            .map(|sst_file| {
                return SSTIterator::open(Path::new(DATA_DIR).join(sst_file).as_path());
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

        let mut new_sst_table: Vec<SSTableEntry> = Vec::with_capacity(2000);
        let mut new_sst_names: Vec<String> = Vec::new();

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

                if new_sst_table.len() == 2000 {
                    // Write all entries to an SST file
                    new_sst_names.push(self.write_sst(&new_sst_table));
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
            new_sst_names.push(self.write_sst(&new_sst_table));
            new_sst_table.clear();
        }

        let mut manifest_entries = self.manifest.write().await;
        *manifest_entries = new_sst_names.clone();

        self.update_manifest_atomic(new_sst_names, false);

        sst_file_paths.iter().for_each(|sst_file| {
            match fs::remove_file(Path::new(DATA_DIR).join(sst_file)) {
                Ok(_) => println!("Removed old sst file {}", sst_file),
                Err(err) => panic!("Unable to remove old sst file {}: {}", sst_file, err),
            };
        });
        println!("Compaction complete")
    }

    async fn write_memtable_to_sst(
        &self,
        store: &mut tokio::sync::RwLockWriteGuard<'_, BTreeMap<String, Option<String>>>,
    ) {
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

        let sst_file_names = vec![self.write_sst(&ss_table)];

        let mut manifest_entries = self.manifest.write().await;
        manifest_entries.extend(sst_file_names.clone());

        self.update_manifest_atomic(sst_file_names, true);
        truncate_wal();

        //Clear MemTable
        store.clear();
    }

    fn write_sst(&self, ss_table: &Vec<SSTableEntry>) -> String {
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

        let mut writer = BufWriter::new(sst_file_descriptor);

        for sst_entry in ss_table {
            serde_json::to_writer(&mut writer, sst_entry).unwrap();
            writer.write_all(b"\n").unwrap();
        }

        writer.flush().unwrap();
        writer.get_ref().sync_all().unwrap();
        File::open(Path::new(DATA_DIR)).unwrap().sync_all().unwrap();
        return sst_file_name;
    }

    fn add_wal_entry(&self, wal_entry: WALEntry) {
        let mut wal_fd = OpenOptions::new()
            .create(true)
            .append(true)
            .open(Path::new(DATA_DIR).join(WAL))
            .unwrap();
        let wal_entry_string = serde_json::to_string(&wal_entry).unwrap() + "\n";
        match wal_fd.write_all(wal_entry_string.as_bytes()) {
            Ok(_) => {
                self.wal_sync_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if self
                    .wal_sync_counter
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 100
                {
                    self.wal_sync_counter
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                    wal_fd.sync_all().unwrap();
                }
            }
            Err(err) => {
                panic!("Unable to write WAL entry {}: {}", wal_entry_string, err)
            }
        };
    }

    fn update_manifest_atomic(&self, sst_file_names: Vec<String>, copy_old_contents: bool) {
        let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
        let manifest_tmp_file_path = Path::new(DATA_DIR).join(MANIFEST_TMP);
        let manifest_tmp_fd = File::create(&manifest_tmp_file_path).unwrap();
        let mut writer = io::BufWriter::new(&manifest_tmp_fd);
        let manifest_fd = File::open(&manifest_file_path).unwrap();

        if copy_old_contents {
            let mut reader = io::BufReader::new(&manifest_fd);
            match io::copy(&mut reader, &mut writer) {
                Ok(_) => {}
                Err(err) => panic!("Unable to copy {} to {}: {}", MANIFEST, MANIFEST_TMP, err),
            };
        }

        for sst_file_name in sst_file_names {
            let _ = writer
                .write(format!("{}\n", sst_file_name).as_bytes())
                .unwrap();
            _ = writer.flush().unwrap();
        }

        _ = manifest_tmp_fd.sync_all().unwrap();

        _ = fs::rename(&manifest_tmp_file_path, &manifest_file_path).unwrap();

        _ = manifest_fd.sync_all().unwrap();
        _ = File::open(&manifest_file_path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }
}

fn truncate_wal() {
    let wal_file_path = Path::new(DATA_DIR).join(WAL);
    match OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&wal_file_path)
    {
        Ok(wal_fd) => {
            let _ = wal_fd.sync_all();
        }
        Err(err) => panic!(
            "Unable to truncate WAL file {}: {}",
            wal_file_path.display(),
            err,
        ),
    };
}

async fn manage_manifest(kvstore: &KVStore, data_dir: &Path) {
    let manifest_file = data_dir.join(MANIFEST);
    match manifest_file.exists() {
        false => {
            kvstore
                .next_sst_id
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
            let mut manifest_entries = kvstore.manifest.write().await;
            manifest_entries.extend(
                fs::read_to_string(manifest_file)
                    .unwrap()
                    .lines()
                    .map(|entry| entry.to_string()),
            );
            manifest_entries.iter().for_each(|sst_file| {
                sst_files.remove(&data_dir.join(sst_file).display().to_string());
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
                Some(last_line) => {
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

            kvstore
                .next_sst_id
                .store(next_sst_id, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

async fn replay_wal(kvstore: &KVStore, data_dir: &Path) {
    let wal_file = data_dir.join(WAL);
    match wal_file.exists() {
        true => {
            let mut store = kvstore.store.write().await;
            let wal_fd = match File::open(&wal_file) {
                Ok(fd) => fd,
                Err(err) => panic!("Unable to open WAL file {}: {}", wal_file.display(), err),
            };
            let reader = io::BufReader::new(wal_fd);
            reader.lines().for_each(|line| {
                let line: String = line.unwrap().trim_end().to_string();
                let wal_entry: WALEntry = serde_json::from_str(&line)
                    .expect(&format!("Failed to parse {} as JSON\n", line));
                match wal_entry.operation.as_str() {
                    "put" => {
                        store.insert(wal_entry.key, Some(wal_entry.value.unwrap()));
                    }
                    "delete" => {
                        store.insert(wal_entry.key, None);
                    }
                    _ => {}
                }
            });
        }
        false => match File::create(&wal_file) {
            Ok(_) => {
                println!("Created WAL file:{}", wal_file.display());
            }
            Err(err) => {
                panic!("Failed to create WAL file {}: {}", wal_file.display(), err);
            }
        },
    };
}

fn is_deleted(value: &Option<String>) -> bool {
    return value.is_none();
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
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!("Failed to open file {:?}: {}", path, err),
        };
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
struct WALEntry {
    operation: String,
    key: String,
    value: Option<String>,
}
