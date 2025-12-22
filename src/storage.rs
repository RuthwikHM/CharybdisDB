use std::collections::HashSet;
use std::fs::OpenOptions;
use std::{
    collections::HashMap,
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
    store: Arc<RwLock<HashMap<String, String>>>,
    next_sst_id: Arc<AtomicU32>,
}

impl KVStore {
    pub async fn new() -> Self {
        let kvstore = Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            next_sst_id: Arc::new(AtomicU32::new(0)),
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
                    fs::remove_file(sst_file).unwrap();
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
                            store.insert(wal_entry.key, wal_entry.value);
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
        return kvstore;
    }

    pub async fn put(&mut self, key: String, value: String) {
        let mut store = self.store.write().await;

        let mut wal_fd = OpenOptions::new()
            .create(true)
            .append(true)
            .open(Path::new(DATA_DIR).join(WAL))
            .unwrap();
        let wal_entry = WALEntry {
            operation: "put".to_string(),
            key: key.clone(),
            value: value.clone(),
        };
        let wal_entry_string = serde_json::to_string(&wal_entry).unwrap() + "\n";
        match wal_fd.write_all(wal_entry_string.as_bytes()) {
            Ok(_) => {
                wal_fd.sync_all().unwrap();
            }
            Err(err) => {
                panic!("Unable to write WAL entry {}: {}", wal_entry_string, err)
            }
        };

        store.insert(key, value);
        if store.len() == 2000 {
            let mut keys: Vec<&String> = store.keys().collect();
            keys.sort();
            let mut ss_table: Vec<SSTableEntry> = Vec::new();
            for key in keys {
                ss_table.push(SSTableEntry {
                    key: key.to_string(),
                    value: store.get(key).unwrap().to_string(),
                });
            }

            // Write SST file
            // let current_sst_id = self.next_sst_id.load(std::sync::atomic::Ordering::SeqCst);
            let current_sst_id = self
                .next_sst_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let sst_file_name = format!("{}-{}.json", SST_FILE_PREFIX, current_sst_id);
            let sst_file_path = Path::new(DATA_DIR).join(&sst_file_name);
            let json_string = match serde_json::to_string(&ss_table) {
                Ok(json_string) => json_string,
                Err(err) => panic!("Couldnt parse MemTable to SSTable:{}", err),
            };
            let mut sst_file_descriptor = match File::create(sst_file_path) {
                Ok(file) => file,
                Err(err) => panic!("Unable to open SSTable file {}: {}", sst_file_name, err),
            };

            match sst_file_descriptor.write_all(json_string.as_bytes()) {
                Ok(_) => {
                    let _ = sst_file_descriptor.sync_all().unwrap();
                    println!("Wrote MemTable contents to file {}", sst_file_name);
                    // self.next_sst_id
                    //     .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    println!(
                        "Next sst-ID:{}",
                        self.next_sst_id.load(std::sync::atomic::Ordering::SeqCst)
                    );
                }
                Err(err) => panic!(
                    "Unable to write MemTable contents to file {}: {}",
                    sst_file_name, err
                ),
            }

            // Append new SST entry to MANIFEST
            let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
            let manifest_tmp_file_path = Path::new(DATA_DIR).join(MANIFEST_TMP);
            let manifest_fd = File::open(&manifest_file_path).unwrap();
            let manifest_tmp_fd = File::create(&manifest_tmp_file_path).unwrap();
            let mut reader = io::BufReader::new(&manifest_fd);
            let mut writer = io::BufWriter::new(&manifest_tmp_fd);
            match io::copy(&mut reader, &mut writer) {
                Ok(_) => {
                    let _ = writer
                        .write(format!("{}\n", sst_file_name).as_bytes())
                        .unwrap();
                    _ = writer.flush().unwrap();

                    _ = manifest_tmp_fd.sync_all().unwrap();

                    _ = fs::rename(&manifest_tmp_file_path, &manifest_file_path).unwrap();

                    _ = manifest_fd.sync_all().unwrap();
                    _ = File::open(&manifest_file_path.parent().unwrap())
                        .unwrap()
                        .sync_all()
                        .unwrap();

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
                Err(err) => panic!("Unable to copy {} to {}: {}", MANIFEST, MANIFEST_TMP, err),
            };

            //Clear MemTable
            store.clear();
        }
    }

    pub async fn get(&mut self, key: &String) -> String {
        let store = self.store.read().await;
        match store.get(key) {
            Some(value) => return value.clone(),
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
                    let sst_file_descriptor = match File::open(sst_file_path) {
                        Ok(file) => file,
                        Err(err) => panic!("Error reading SST file {}: {}", sst_file, err),
                    };
                    let sst_file_contents: Vec<SSTableEntry> =
                        serde_json::from_reader(sst_file_descriptor).expect(&format!(
                            "Failed to parse contents of {} into as JSON",
                            sst_file
                        ));

                    for entry in sst_file_contents {
                        if entry.key == comp_key {
                            return entry.value;
                        }
                    }
                }
                return "".to_string();
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SSTableEntry {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct WALEntry {
    operation: String,
    key: String,
    value: String,
}
