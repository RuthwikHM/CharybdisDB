use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter};
use std::sync::atomic::AtomicU16;
use std::{
    collections::HashMap,
    fs::{self, File, create_dir},
    io::{self, BufRead, Write},
    path::Path,
    sync::{Arc, atomic::AtomicU32},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockWriteGuard};

const DATA_DIR: &str = "data";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const SST_FILE_PREFIX: &str = "sst";
const WAL: &str = "wal.db";

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<RwLock<HashMap<String, Option<String>>>>,
    next_sst_id: Arc<AtomicU32>,
    num_updates: Arc<AtomicU16>,
}

impl KVStore {
    pub async fn new() -> Self {
        let kvstore = Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            next_sst_id: Arc::new(AtomicU32::new(0)),
            num_updates: Arc::new(AtomicU16::new(0)),
        };
        let data_dir = Path::new(DATA_DIR);
        if !data_dir.exists() {
            match create_dir(data_dir) {
                Ok(_) => {
                    println!("Created data directory:{}", DATA_DIR);
                    kvstore
                        .next_sst_id
                        .store(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(err) => {
                    panic!("Failed to create data directory: {}", err)
                }
            };
        }
        manage_manifest(&kvstore, data_dir);

        replay_wal(&kvstore, data_dir).await;
        return kvstore;
    }

    pub async fn put(&self, key: String, value: String) {
        let mut store = self.store.write().await;

        let wal_entry = WALEntry {
            operation: "put".to_string(),
            key: key.clone(),
            value: value.clone(),
        };
        self.add_wal_entry(wal_entry);

        store.insert(key, Some(value));

        if store.len() == 2000 {
            self.write_memtable_to_sst(&mut store);
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.num_updates.load(std::sync::atomic::Ordering::SeqCst) == 10000 {
            self.compact_sst(&store);
            self.num_updates
                .store(0, std::sync::atomic::Ordering::SeqCst);
        }

        drop(store);
    }

    pub async fn delete(&self, key: String) {
        let mut store = self.store.write().await;

        let wal_entry = WALEntry {
            operation: "delete".to_string(),
            key: key.clone(),
            value: String::new(),
        };
        self.add_wal_entry(wal_entry);

        store.insert(key, None);

        if store.len() == 2000 {
            self.write_memtable_to_sst(&mut store);
        }

        self.num_updates
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.num_updates.load(std::sync::atomic::Ordering::SeqCst) == 10000 {
            self.compact_sst(&store);
            self.num_updates
                .store(0, std::sync::atomic::Ordering::SeqCst);
        }
        drop(store);
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
                let comp_key = key.to_string();
                let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
                let manifest_content = match fs::read_to_string(manifest_file_path) {
                    Ok(content) => content,
                    Err(err) => panic!("Error reading MANIFEST file: {}", err),
                };
                let sst_files: Vec<&str> = manifest_content.lines().collect();
                for sst_file in sst_files.iter().rev() {
                    let sst_file_path = Path::new(DATA_DIR).join(sst_file);
                    let entries: Vec<SSTableEntry> = SSTIterator::open(sst_file_path.as_path())
                        .lines
                        .map(|line| {
                            let line_str = line.unwrap();
                            match serde_json::from_str(&line_str) {
                                Ok(sst_entry) => sst_entry,
                                Err(err) => panic!("Failed to parse {}: {}", line_str, err),
                            }
                        })
                        .collect();
                    let size: usize = entries.len();
                    let mut l: usize = 0;
                    let mut r: usize = size - 1;
                    let mut mid: usize;
                    while l <= r {
                        mid = l + (r - l) / 2;
                        let entry: &SSTableEntry = &entries[mid];
                        match &entry.key {
                            key if *key < comp_key => {
                                if mid == size - 1 {
                                    break;
                                }
                                l = mid + 1;
                            }
                            key if *key > comp_key => {
                                if mid == 0 {
                                    break;
                                }
                                r = mid - 1;
                            }
                            key if *key == comp_key => {
                                if entry.deleted {
                                    // Tombstone i.e. deleted value
                                    return "".to_string();
                                } else {
                                    return entry.value.clone();
                                }
                            }
                            _ => {}
                        }
                    }
                }
                return "".to_string();
            }
        }
    }

    fn compact_sst(&self, _: &RwLockWriteGuard<'_, HashMap<String, Option<String>>>) {
        println!("Compacting SSTs");
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
        let manifest_content_itr = match fs::read_to_string(manifest_file_path) {
            Ok(content) => content,
            Err(err) => panic!("Error reading MANIFEST file: {}", err),
        };
        let sst_file_paths: Vec<&str> = manifest_content_itr.lines().collect();
        let mut sst_itrs: Vec<SSTIterator> = sst_file_paths
            .iter()
            .map(|sst_file| {
                return SSTIterator::open(Path::new(DATA_DIR).join(sst_file).as_path());
            })
            .collect();

        for (i, itr) in sst_itrs.iter_mut().enumerate() {
            if let Some(sst_entry) = itr.next() {
                let heap_entry = HeapEntry {
                    key: sst_entry.key.to_string(),
                    value: sst_entry.value.to_string(),
                    deleted: sst_entry.deleted,
                    sst_table_pos: i,
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
            if !newest.deleted {
                new_sst_table.push(SSTableEntry {
                    key: newest.key.clone(),
                    value: newest.value.clone(),
                    deleted: false,
                });

                if new_sst_table.len() == 2000 {
                    // Write all entries to an SST file
                    new_sst_names.push(self.write_sst(&new_sst_table));
                    new_sst_table.clear();
                }
            }

            for entry in drained {
                let itr = &mut sst_itrs[entry.sst_table_pos];
                if let Some(sst_entry) = itr.next() {
                    let heap_entry = HeapEntry {
                        key: sst_entry.key.to_string(),
                        value: sst_entry.value.to_string(),
                        deleted: sst_entry.deleted,
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

        self.update_manifest_atomic(new_sst_names, false);

        sst_file_paths.iter().for_each(|sst_file| {
            match fs::remove_file(Path::new(DATA_DIR).join(sst_file)) {
                Ok(_) => println!("Removed old sst file {}", sst_file),
                Err(err) => panic!("Unable to remove old sst file {}: {}", sst_file, err),
            };
        });
        println!("Compaction complete")
    }

    fn write_memtable_to_sst(
        &self,
        store: &mut tokio::sync::RwLockWriteGuard<'_, HashMap<String, Option<String>>>,
    ) {
        let mut keys: Vec<&String> = store.keys().collect();
        keys.sort();
        let mut ss_table: Vec<SSTableEntry> = Vec::with_capacity(keys.len());
        for key in keys {
            match store.get(key).unwrap() {
                Some(value) => {
                    ss_table.push(SSTableEntry {
                        key: key.to_string(),
                        value: value.to_string(),
                        deleted: false,
                    });
                }
                None => {
                    ss_table.push(SSTableEntry {
                        key: key.to_string(),
                        value: String::new(),
                        deleted: true,
                    });
                }
            };
        }

        let sst_file_names = vec![self.write_sst(&ss_table)];

        self.update_manifest_atomic(sst_file_names, true);

        //Clear MemTable
        store.clear();
    }

    fn write_sst(&self, ss_table: &Vec<SSTableEntry>) -> String {
        let current_sst_id = self
            .next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                wal_fd.sync_all().unwrap();
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

        truncate_wal();
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

fn manage_manifest(kvstore: &KVStore, data_dir: &Path) {
    let manifest_file = data_dir.join(MANIFEST);
    match manifest_file.exists() {
        false => {
            kvstore
                .next_sst_id
                .store(1, std::sync::atomic::Ordering::SeqCst);
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
            let manifest_entries: Vec<String> = fs::read_to_string(manifest_file)
                .unwrap()
                .lines()
                .map(|entry| entry.to_string())
                .collect();
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
                .store(next_sst_id, std::sync::atomic::Ordering::SeqCst);
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
                        store.insert(wal_entry.key, Some(wal_entry.value));
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

#[derive(Debug)]
struct HeapEntry {
    key: String,
    value: String,
    deleted: bool,
    sst_table_pos: usize,
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
    value: String,
    deleted: bool,
}

struct SSTIterator {
    lines: io::Lines<io::BufReader<File>>,
}

impl SSTIterator {
    fn open(path: &Path) -> Self {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!("Failed to open file {:?}: {}", path, err),
        };
        let reader = BufReader::new(file);
        return Self {
            lines: reader.lines(),
        };
    }

    fn next(&mut self) -> Option<SSTableEntry> {
        match self.lines.next() {
            Some(line) => serde_json::from_str(&line.unwrap()).unwrap(),
            None => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct WALEntry {
    operation: String,
    key: String,
    value: String,
}
