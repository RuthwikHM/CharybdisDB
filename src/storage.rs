use crate::bloom_filter::{BLOOM_FILTER_SUFFIX, BLOOM_FP_RATE, BloomFilter};
use crate::utils::{
    CompactionJob, DATA_DIR, HeapEntry, L0, LEVEL_PREFIX, MANIFEST, MANIFEST_TMP, NUM_LEVELS,
    Range, SPARSE_INDEX_SUFFIX, SST_FILE_PREFIX, SSTIterator, SSTLevelMetadata, SSTMetadata,
    SSTableEntry, SparseIndex, SparseIndexEntry, WAL, WALEntry, WALOp, is_deleted,
    remove_file_extension,
};
use rkyv::from_bytes;
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::{
    fs::{self, File, create_dir},
    io::{self, BufRead, Write},
    path::Path,
    sync::{Arc, atomic::AtomicU32},
};
use tokio::sync::{RwLock, mpsc};

const MEMTABLE_LIMIT: usize = 2000;
const WAL_FSYNC_EVERY: u8 = 100;

#[derive(Debug, Clone)]
pub struct KVStore {
    store: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    manifest: Arc<Vec<SSTLevelMetadata>>,
    wal_fd: Arc<Mutex<File>>,
    wal_write_counter: Arc<AtomicU8>,
    compaction_tx: mpsc::Sender<CompactionJob>,
}

impl KVStore {
    pub async fn new() -> Self {
        let data_dir = Path::new(DATA_DIR);
        let l0_dir = data_dir.join(L0);
        let l1_dir = data_dir.join(format!("{}{}", LEVEL_PREFIX, 1));
        if !data_dir.exists() {
            match create_dir(data_dir) {
                Ok(_) => {
                    println!("Created data directory:{}", DATA_DIR);
                    create_dir(l0_dir).unwrap();
                    create_dir(l1_dir).unwrap();
                }
                Err(err) => {
                    panic!("Failed to create data directory: {}", err)
                }
            };
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

        let (compaction_tx, mut compaction_rx) = mpsc::channel::<CompactionJob>(8);
        let mut kvstore = Self {
            store: Arc::new(RwLock::new(BTreeMap::new())),
            manifest: Arc::default(),
            wal_fd: Arc::new(Mutex::new(wal_fd)),
            wal_write_counter: Arc::new(AtomicU8::new(0)),
            compaction_tx: compaction_tx,
        };
        kvstore.manage_manifest(data_dir).await;

        kvstore.replay_wal().await;

        let worker_clone = kvstore.clone();
        tokio::spawn(async move {
            println!("Started background worker for compaction");
            while let Some(job) = compaction_rx.recv().await {
                match job {
                    CompactionJob::CompactLevel(level) => {
                        let level_idx = level as usize;
                        let mut current_size: u32 = 0;
                        {
                            let guard = worker_clone.manifest[level_idx].entries.read().await;
                            current_size = guard.len().try_into().unwrap();
                        }
                        let file_limit = get_level_limit(level);
                        if current_size >= file_limit {
                            worker_clone.compact_sst(level).await;
                        }
                    }
                }
            }
        });
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
                let sst_files_metadata: Vec<SSTMetadata> = {
                    let guard = self.manifest[0].entries.read().await;
                    // println!("Get Acquired l0 entries guard");
                    guard
                        .iter()
                        .map(|sst_metadata| sst_metadata.clone())
                        .collect()
                };
                // println!("Get Dropped l0 entries guard");
                let data_dir_path = Path::new(DATA_DIR);

                // Check L0
                for sst_file_meta in sst_files_metadata.iter().rev() {
                    let bloom_filter = sst_file_meta.bloom_filter.as_ref().unwrap();

                    if !bloom_filter.might_contain(comp_key) {
                        continue;
                    }

                    let sst_file_path = data_dir_path.join(L0).join(&sst_file_meta.file_name);

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

                // Check lower levels
                for i in 1..NUM_LEVELS {
                    let level: usize = i.try_into().unwrap();
                    let sst_entries: Vec<SSTMetadata> = {
                        let guard = self.manifest[level].entries.read().await;
                        guard
                            .iter()
                            .map(|sst_metadata| sst_metadata.clone())
                            .collect()
                    };
                    let valid_sst_entries: Vec<&SSTMetadata> = sst_entries
                        .iter()
                        .filter(|sst_entry| {
                            let lower_bound_check =
                                comp_key.cmp(sst_entry.min_key.as_ref().unwrap());
                            let upper_bound_check =
                                comp_key.cmp(sst_entry.max_key.as_ref().unwrap());
                            return (lower_bound_check == Ordering::Greater
                                || lower_bound_check == Ordering::Equal)
                                && (upper_bound_check == Ordering::Less
                                    || upper_bound_check == Ordering::Equal);
                        })
                        .collect();
                    // Move to the next level if no matches here
                    if valid_sst_entries.len() == 0 {
                        continue;
                    }

                    assert!(
                        valid_sst_entries.len() == 1,
                        "More than one SST found at level {} for key {}",
                        i,
                        key
                    );

                    let sst_entry = valid_sst_entries[0];
                    let sst_file_path = data_dir_path
                        .join(format!("{}{}", LEVEL_PREFIX, level))
                        .join(&sst_entry.file_name);
                    for entry in SSTIterator::open(&sst_file_path) {
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
                            // Note this should not ideally happen here
                            Ordering::Greater => break,
                        };
                    }
                }

                return "".to_string();
            }
        }
    }

    pub async fn scan(&self, _range: Range) -> Vec<String> {
        let result: Vec<String> = Vec::new();
        return result;
    }

    // FIXME compaction logic is breaking now possibly due to me messing with the RWGuards.
    async fn compact_sst(&self, start_level: u32) {
        println!("Compacting SSTs");
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        let mut old_sst_file_paths: Vec<Vec<String>> = Vec::new();
        let level_idx: usize = start_level.try_into().unwrap();
        let lower_level_idx = level_idx + 1;

        {
            let mut level_guard = self.manifest[level_idx].entries.read().await;
            old_sst_file_paths.push(
                level_guard
                    .iter()
                    .map(|sst_metadata| sst_metadata.file_name.clone())
                    .collect(),
            );

            if level_idx + 1 < self.manifest.len() {
                level_guard = self.manifest[lower_level_idx].entries.read().await;
                old_sst_file_paths.push(
                    level_guard
                        .iter()
                        .map(|sst_metadata| sst_metadata.file_name.clone())
                        .collect(),
                );
            }
        }

        println!("Gathered SST file names");

        // Collect old sst file names and drop read guard
        let mut sst_itrs: Vec<Vec<SSTIterator>> = old_sst_file_paths
            .iter()
            .enumerate()
            .map(|(idx, entries)| {
                entries
                    .iter()
                    .map(|sst_file| {
                        return SSTIterator::open(
                            Path::new(DATA_DIR)
                                .join(format!("{}{}", LEVEL_PREFIX, start_level + idx as u32))
                                .join(&sst_file)
                                .as_path(),
                        );
                    })
                    .collect()
            })
            .collect();

        for (level, entries) in sst_itrs.iter_mut().enumerate() {
            for (i, itr) in entries.iter_mut().enumerate() {
                if let Some(sst_entry) = itr.next() {
                    let heap_entry = HeapEntry {
                        key: sst_entry.key,
                        value: sst_entry.value,
                        sst_table_pos: i as u64,
                        level: start_level + level as u32,
                    };
                    heap.push(heap_entry);
                }
            }
        }
        println!("File iterators pushed to heap");

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
                    new_sst_files_metadata.push(self.write_sst(&new_sst_table, start_level + 1));
                    new_sst_table.clear();
                }
            }

            for entry in drained {
                let level: usize = entry.level.try_into().unwrap();
                let idx: usize = entry.sst_table_pos.try_into().unwrap();
                let itr = &mut sst_itrs[level][idx];
                if let Some(sst_entry) = itr.next() {
                    let heap_entry = HeapEntry {
                        key: sst_entry.key.to_string(),
                        value: sst_entry.value,
                        sst_table_pos: entry.sst_table_pos,
                        level: entry.level,
                    };
                    heap.push(heap_entry);
                }
            }
        }

        if new_sst_table.len() != 0 {
            new_sst_files_metadata.push(self.write_sst(&new_sst_table, start_level + 1));
            new_sst_table.clear();
        }

        println!("Done with merge");

        {
            let mut higher_level_entries_guard = self.manifest[level_idx].entries.write().await;
            // println!("Compact acquired level {} guard", level_idx);
            let mut lower_level_entries_guard =
                self.manifest[lower_level_idx].entries.write().await;
            // println!("Compact acquired level {} guard", level_idx + 1);
            // Clear previous level, assign new SSTs to next level
            higher_level_entries_guard.clear();
            self.manifest[level_idx]
                .next_sst_id
                .store(1, std::sync::atomic::Ordering::Relaxed);
            *lower_level_entries_guard = new_sst_files_metadata;

            let mut level_file_names: Vec<Vec<String>> = Vec::new();
            for i in 0..NUM_LEVELS {
                let level: usize = i.try_into().unwrap();
                if level == level_idx {
                    level_file_names.push(
                        higher_level_entries_guard
                            .iter()
                            .map(|sst_metadata| match level == 0 {
                                true => sst_metadata.file_name.clone(),
                                false => {
                                    format!(
                                        "{}-{}:{}",
                                        sst_metadata.min_key.as_ref().unwrap(),
                                        sst_metadata.max_key.as_ref().unwrap(),
                                        &sst_metadata.file_name
                                    )
                                }
                            })
                            .collect(),
                    );
                } else if level == lower_level_idx {
                    level_file_names.push(
                        lower_level_entries_guard
                            .iter()
                            .map(|sst_metadata| match level == 0 {
                                true => sst_metadata.file_name.clone(),
                                false => {
                                    format!(
                                        "{}-{}:{}",
                                        sst_metadata.min_key.as_ref().unwrap(),
                                        sst_metadata.max_key.as_ref().unwrap(),
                                        &sst_metadata.file_name
                                    )
                                }
                            })
                            .collect(),
                    );
                } else {
                    let level_entries_guard = self.manifest[level].entries.read().await;
                    let level_entry_filenames: Vec<String> = level_entries_guard
                        .iter()
                        .map(|sst_metadata| match level == 0 {
                            true => sst_metadata.file_name.clone(),
                            false => {
                                format!(
                                    "{}-{}:{}",
                                    sst_metadata.min_key.as_ref().unwrap(),
                                    sst_metadata.max_key.as_ref().unwrap(),
                                    &sst_metadata.file_name
                                )
                            }
                        })
                        .collect();
                    level_file_names.push(level_entry_filenames);
                    drop(level_entries_guard);
                }
            }

            println!("Updating manifest on disk");
            self.update_manifest_atomic(level_file_names);
            // println!("Compact dropped level {} guard", level_idx);
            // println!("Compact dropped level {} guard", level_idx + 1);
        }

        old_sst_file_paths
            .iter()
            .enumerate()
            .for_each(|(level, entries)| {
                entries.iter().for_each(|sst_file| {
                    let level_path = Path::new(DATA_DIR).join(format!(
                        "{}{}",
                        LEVEL_PREFIX,
                        start_level + level as u32
                    ));
                    match fs::remove_file(level_path.join(&sst_file)) {
                        Ok(_) => println!("Removed old sst file {}", sst_file),
                        Err(err) => panic!("Unable to remove old sst file {}: {}", sst_file, err),
                    };
                    if level == 0 {
                        let sst_idx_name = format!(
                            "{}.{}",
                            remove_file_extension(&sst_file),
                            SPARSE_INDEX_SUFFIX
                        );
                        match fs::remove_file(level_path.join(&sst_idx_name)) {
                            Ok(_) => println!(
                                "Removed old sst index file {} from level {}",
                                &sst_idx_name, level
                            ),
                            Err(err) => {
                                panic!("Unable to remove old sst index file {}: {}", sst_file, err)
                            }
                        };
                        let sst_bloom_name = format!(
                            "{}.{}",
                            remove_file_extension(&sst_file),
                            BLOOM_FILTER_SUFFIX
                        );
                        match fs::remove_file(level_path.join(&sst_bloom_name)) {
                            Ok(_) => println!(
                                "Removed old sst index file {} from level {}",
                                &sst_bloom_name, level
                            ),
                            Err(err) => {
                                panic!("Unable to remove old sst index file {}: {}", sst_file, err)
                            }
                        };
                    }
                });
            });
        File::open(Path::new(DATA_DIR)).unwrap().sync_all().unwrap();
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

        let sst_files_metadata = vec![self.write_sst(&ss_table, 0)];

        // Update SST L0 entries with the new table
        {
            let mut manifest_entries = self.manifest[0].entries.write().await;
            // println!("Write mem table acquired level 0 manifest lock");
            manifest_entries.extend(sst_files_metadata);
        }
        // println!("Write mem table dropped level 0 manifest lock");

        let manifest_entries = self.get_manifest_entry_names().await;
        self.update_manifest_atomic(manifest_entries);
        // drop(manifest_entries);
        let mut l0_size: u32 = {
            // println!("Write mem table compact acquired level 0 manifest lock");
            let manifest_read_guard = self.manifest[0].entries.read().await;
            manifest_read_guard.len() as u32
        };
        // println!("Write mem table compact dropped level 0 manifest lock");
        // Trigger compaction if needed
        if l0_size == 5 {
            match self.compaction_tx.try_send(CompactionJob::CompactLevel(0)) {
                Ok(_) => {
                    println!("Queued compaction job at level 0");
                }
                Err(err) => {
                    println!("Failed to queue compaction job: {}", err);
                }
            };
        }

        //Clear MemTable
        store.clear();

        // Truncate WAL after SSTs are persisted and Manifest is updated
        self.truncate_wal();
    }

    async fn get_manifest_entry_names(&self) -> Vec<Vec<String>> {
        let mut manifest_data: Vec<Vec<String>> = Vec::new();
        for i in 0..NUM_LEVELS {
            let level: usize = i.try_into().unwrap();
            let level_entries_guard = self.manifest[level].entries.read().await;
            let level_entry_filenames: Vec<String> = level_entries_guard
                .iter()
                .map(|sst_metadata| match level == 0 {
                    true => sst_metadata.file_name.clone(),
                    false => {
                        format!(
                            "{}-{}:{}",
                            sst_metadata.min_key.as_ref().unwrap(),
                            sst_metadata.max_key.as_ref().unwrap(),
                            &sst_metadata.file_name
                        )
                    }
                })
                .collect();
            manifest_data.push(level_entry_filenames);
            drop(level_entries_guard);
        }
        return manifest_data;
    }

    fn write_sst(&self, ss_table: &Vec<SSTableEntry>, level: u32) -> SSTMetadata {
        let idx: usize = level.try_into().unwrap();
        let current_sst_id: u32 = self.manifest[idx]
            .next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sst_file_name = format!("{}-{}.json", SST_FILE_PREFIX, current_sst_id);
        let level_path = Path::new(DATA_DIR).join(format!("{}{}", LEVEL_PREFIX, level));
        let sst_file_path = level_path.join(&sst_file_name);
        let sst_file_descriptor = match File::create(sst_file_path) {
            Ok(file) => file,
            Err(err) => panic!("Unable to open SSTable file {}: {}", sst_file_name, err),
        };

        return match level == 0 {
            true => {
                let mut index = SparseIndex { entries: vec![] };
                let mut bloom_filter = BloomFilter::new(ss_table.len(), BLOOM_FP_RATE);
                let mut entry_count = 0;
                let mut offset: u64 = 0;

                let mut writer = BufWriter::new(sst_file_descriptor);

                for sst_entry in ss_table {
                    bloom_filter.insert(&sst_entry.key);

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
                let idx_path = level_path.join(idx_file_name);
                let mut idx_fd = File::create(idx_path).unwrap();

                let idx_bytes = rkyv::to_bytes::<Error>(&index).unwrap();
                idx_fd.write_all(&idx_bytes).unwrap();
                idx_fd.sync_all().unwrap();

                // Persist bloom filter
                let bloom_file_name = format!(
                    "{}-{}.{}",
                    SST_FILE_PREFIX, current_sst_id, BLOOM_FILTER_SUFFIX
                );
                let bloom_path = level_path.join(bloom_file_name);
                let mut bloom_fd = File::create(bloom_path).unwrap();

                let bloom_bytes = rkyv::to_bytes::<Error>(&bloom_filter).unwrap();
                bloom_fd.write_all(&bloom_bytes).unwrap();
                bloom_fd.sync_all().unwrap();

                File::open(level_path).unwrap().sync_all().unwrap();

                SSTMetadata {
                    file_name: sst_file_name,
                    index: Some(Arc::new(index)),
                    bloom_filter: Some(Arc::new(bloom_filter)),
                    min_key: None,
                    max_key: None,
                }
            }
            false => {
                let mut writer = BufWriter::new(sst_file_descriptor);

                for sst_entry in ss_table {
                    let line = serde_json::to_string(sst_entry).unwrap();
                    writer.write_all(format!("{}\n", line).as_bytes()).unwrap();
                }

                // Fsync SSTable
                writer.flush().unwrap();
                writer.get_ref().sync_all().unwrap();
                File::open(level_path).unwrap().sync_all().unwrap();

                let min_key = ss_table.first().unwrap().key.clone();
                let max_key = ss_table.last().unwrap().key.clone();

                SSTMetadata {
                    file_name: sst_file_name,
                    index: None,
                    bloom_filter: None,
                    min_key: Some(min_key),
                    max_key: Some(max_key),
                }
            }
        };
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
                    wal_fd.sync_all().unwrap();
                    self.wal_write_counter
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Err(err) => {
                panic!("Unable to write WAL entry {}: {}", wal_entry_string, err)
            }
        };
    }

    fn update_manifest_atomic(&self, manifest_entries: Vec<Vec<String>>) {
        let manifest_file_path = Path::new(DATA_DIR).join(MANIFEST);
        let manifest_tmp_file_path = Path::new(DATA_DIR).join(MANIFEST_TMP);
        let manifest_tmp_fd = File::create(&manifest_tmp_file_path).unwrap();
        let mut writer = io::BufWriter::new(&manifest_tmp_fd);

        for level in 0..NUM_LEVELS {
            writer.write(format!("[L{}]\n", level).as_bytes()).unwrap();
            let idx: usize = level.try_into().unwrap();
            manifest_entries[idx].iter().for_each(|sst_entry_filename| {
                writer
                    .write(format!("{}\n", &sst_entry_filename).as_bytes())
                    .unwrap();
            });
            writer.flush().unwrap();
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

            match wal_entry.operation {
                WALOp::PUT => store.insert(wal_entry.key, wal_entry.value),
                WALOp::DELETE => store.insert(wal_entry.key, None),
            };
        }

        // Truncate WAL after replaying
        wal_fd.set_len(0).unwrap();
        wal_fd.sync_all().unwrap();
    }

    async fn manage_manifest(&mut self, data_dir: &Path) {
        let manifest_file = data_dir.join(MANIFEST);
        // Initialize in memory MANIFEST empty representation
        let mut manifest_entries: Vec<SSTLevelMetadata> = Vec::new();
        for level in 0..NUM_LEVELS {
            manifest_entries.push(SSTLevelMetadata {
                next_sst_id: Arc::new(AtomicU32::new(1)),
                entries: Arc::new(RwLock::new(Vec::new())),
            });
            let level_dir_path = data_dir.join(format!("{}{}", LEVEL_PREFIX, level));
            if !fs::exists(&level_dir_path).unwrap() {
                fs::create_dir(level_dir_path).unwrap();
            }
        }
        match manifest_file.exists() {
            false => {
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
                // Acquire write guards for each level so we can append Manifest representation
                let mut level_entries_guards: Vec<tokio::sync::RwLockWriteGuard<Vec<SSTMetadata>>> =
                    Vec::new();

                let mut sst_files: HashSet<String> = Default::default();
                for level in 0..NUM_LEVELS {
                    let idx: usize = level.try_into().unwrap();
                    level_entries_guards.push(manifest_entries[idx].entries.write().await);

                    sst_files.extend(
                        fs::read_dir(data_dir.join(format!("{}{}", LEVEL_PREFIX, level)))
                            .unwrap()
                            .map(|file| file.unwrap().path().display().to_string())
                            .collect::<HashSet<String>>(),
                    );
                }

                let mut level: usize = 0;

                // Parse manifest entries into in memory representation
                fs::read_to_string(manifest_file)
                    .unwrap()
                    .lines()
                    .for_each(|line| {
                        if line.starts_with("[L") {
                            let p1 = line.find("L").unwrap() + 1;
                            let p2 = line.find("]").unwrap();
                            level = usize::from_str(line.get(p1..p2).unwrap()).unwrap();
                        } else {
                            match level == 0 {
                                true => {
                                    let sst_file = remove_file_extension(line);
                                    let index_file_name =
                                        format!("{}.{}", sst_file, SPARSE_INDEX_SUFFIX);
                                    let mut index_fd = File::open(
                                        Path::new(DATA_DIR).join(L0).join(index_file_name),
                                    )
                                    .unwrap();
                                    let mut index_content_bytes: AlignedVec<16> = AlignedVec::new();
                                    index_content_bytes
                                        .extend_from_reader(&mut index_fd)
                                        .unwrap();
                                    let index =
                                        from_bytes::<SparseIndex, Error>(&index_content_bytes)
                                            .unwrap();

                                    let bloom_filter_file_name =
                                        format!("{}.{}", sst_file, BLOOM_FILTER_SUFFIX);
                                    let mut bloom_fd = File::open(
                                        Path::new(DATA_DIR).join(L0).join(bloom_filter_file_name),
                                    )
                                    .unwrap();
                                    let mut bloom_content_bytes: AlignedVec<16> = AlignedVec::new();
                                    bloom_content_bytes
                                        .extend_from_reader(&mut bloom_fd)
                                        .unwrap();
                                    let bloom_filter =
                                        from_bytes::<BloomFilter, Error>(&bloom_content_bytes)
                                            .unwrap();

                                    level_entries_guards[level].push(SSTMetadata {
                                        file_name: line.to_string(),
                                        index: Some(Arc::new(index)),
                                        bloom_filter: Some(Arc::new(bloom_filter)),
                                        min_key: None,
                                        max_key: None,
                                    });
                                }
                                false => {
                                    // Entries like a-b: sst-1.json
                                    let semicolon_idx = line.find(":").unwrap();
                                    let hypen_idx = line.find("-").unwrap();
                                    let min_key = String::from(line.get(0..hypen_idx).unwrap());
                                    let max_key = String::from(
                                        line.get(hypen_idx + 1..semicolon_idx).unwrap(),
                                    );
                                    let sst_file_name = line.get(semicolon_idx + 1..).unwrap();

                                    level_entries_guards[level].push(SSTMetadata {
                                        file_name: sst_file_name.to_string(),
                                        index: None,
                                        bloom_filter: None,
                                        min_key: Some(min_key),
                                        max_key: Some(max_key),
                                    });
                                }
                            }
                        }
                    });

                // Mark valid SSTs to prevent deletion
                level_entries_guards
                    .iter()
                    .enumerate()
                    .for_each(|(level, sst_level_meta)| {
                        sst_level_meta.iter().for_each(|sst_file_meta| {
                            sst_files.remove(
                                &data_dir
                                    .join(format!("{}{}", LEVEL_PREFIX, level))
                                    .join(&sst_file_meta.file_name)
                                    .display()
                                    .to_string(),
                            );
                        });
                    });

                sst_files.iter().for_each(|sst_file| {
                    match fs::remove_file(sst_file) {
                        Ok(_) => {}
                        Err(err) => {
                            panic!("Unable to remove dangling SST file {}: {}", sst_file, err)
                        }
                    };
                });
                // Find next available SST ID for each level
                for i in 0..NUM_LEVELS {
                    let level: usize = i.try_into().unwrap();
                    let next_sst_id: u32 = match level_entries_guards[level].last() {
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
                    manifest_entries[level]
                        .next_sst_id
                        .store(next_sst_id, std::sync::atomic::Ordering::Relaxed);
                }
                // println!(
                //     "Dir:{:?}\nManifest:{:?}\n Last:{}",
                //     sst_files, manifest_entries, last_line
                // );
                // Dropping guards on the SST entries Vec at each level
                drop(level_entries_guards);
            }
        }

        self.manifest = Arc::new(manifest_entries);
    }
}

fn get_level_limit(level: u32) -> u32 {
    match level {
        0 => 5,
        _ => 5 + (5 * level),
    }
    // 1 => 10,
    // _ => 10 * (10_usize.pow(level - 1)), // General growth factor (10x per level)
}

fn find_sparse_idx_offset(comp_key: &str, sst_file_meta: &SSTMetadata) -> u64 {
    let entries = &sst_file_meta.index.as_ref().unwrap().entries;
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
