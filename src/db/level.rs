use super::compact::CompactStatus;
use super::options::Options;
use crate::file::manifest::ManifestFile;
use crate::table::table::Table;
use crate::utils::file::file_helper;
use crate::utils::slice::Slice;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

pub type Level = Arc<RwLock<LevelHandler>>;
pub(crate) struct LevelManager {
    pub(crate) max_fid: AtomicU64,
    pub(crate) opt: Arc<Options>,
    pub(crate) manifest_file: RwLock<ManifestFile>,
    pub(crate) levels: Vec<Level>,
    pub(crate) compact_state: RwLock<CompactStatus>,
}

#[derive(Default)]
pub(crate) struct LevelHandler {
    pub(crate) level_num: u32,
    pub(crate) tables: Vec<Table>,
    pub(crate) total_size: u64,
}

impl LevelManager {
    pub fn new(opt: Arc<Options>) -> Result<LevelManager, String> {
        let manifest_file = ManifestFile::open(opt.clone())
            .map_err(|e| format!("failed to open the manifest, {}", e))?;

        let id_set = file_helper::load_id_set(&opt.work_dir)
            .map_err(|e| format!("failed to load id set, {}", e))?;

        // verify the correctness of the manifest file
        manifest_file.revert(id_set)?;

        let levels: Vec<RwLock<LevelHandler>> = (0..opt.max_level_num)
            .map(|_| RwLock::new(LevelHandler::default()))
            .collect();

        let manifest = manifest_file.get_manifest();
        let mut max_fid = 0;

        for (&fid, table_info) in &manifest.tables {
            let file_name = file_helper::file_sstable_name(fid);
            max_fid = std::cmp::max(max_fid, fid);

            let table = Table::Open(opt.clone(), file_name, None)
                .map_err(|e| format!("faild to open the table {}, {}", &fid, e))?;

            let mut level = levels[table_info.level as usize]
                .write()
                .map_err(|e| format!("failed to lock level for writing, {}", e))?;

            level.add(table);
        }

        for level in &levels {
            let mut level = level.write().map_err(|e| e.to_string())?;

            level.sort();
        }
        let levels = levels.into_iter().map(|x| Arc::new(x)).collect();

        Ok(LevelManager {
            max_fid: AtomicU64::new(max_fid),
            opt,
            manifest_file: RwLock::new(manifest_file),
            levels,
            compact_state: RwLock::new(CompactStatus::default()),
        })
    }

    pub fn get_level_num_tables(&self, idx: usize) -> u32 {
        self.levels[idx as usize].read().unwrap().tables.len() as u32
    }

    pub fn get_level_total_size(&self, idx: usize) -> u64 {
        self.levels[idx as usize].read().unwrap().total_size
    }

    // to replace level tables
    pub fn replace_level_tables(
        &self,
        level: u32,
        del_tables: &Vec<u32>,
        mut new_tables: Vec<Table>,
    ) {
        let mut level = self.levels[level as usize].write().unwrap();
        for i in del_tables {
            level.tables.remove(*i as usize);
            level.total_size -= level.tables[*i as usize].size();
        }

        level.tables.append(&mut new_tables);
        level.sort();
    }

    // to delete level tables
    pub fn delete_level_tables(&self, level: u32, del_tables: &Vec<u32>) {
        let mut level = self.levels[level as usize].write().unwrap();
        for i in del_tables {
            level.tables.remove(*i as usize);
            level.total_size -= level.tables[*i as usize].size();
        }
    }

    // get val form the key
    pub fn get(&self, key: &[u8]) -> Option<Slice> {
        if let Some(val) = self.search_L0_sst(key) {
            return Some(val);
        }
        for i in 1..self.opt.max_level_num {
            if let Some(val) = self.search_ln_sst(i, key) {
                return Some(val);
            }
        }

        None
    }

    // search key in L0 ssts
    pub fn search_L0_sst(&self, key: &[u8]) -> Option<Slice> {
        let tables = &self.levels[0].read().unwrap().tables;
        for table in tables {
            let mut iter = table.new_iterator();
            if let Some(val) = iter.seek(key) {
                return Some(val.clone());
            }
        }
        None
    }

    // search key in LN ssts
    pub fn search_ln_sst(&self, level: u32, key: &[u8]) -> Option<Slice> {
        let tables = &self.levels[level as usize].read().unwrap().tables;
        if tables.is_empty() {
            return None;
        }

        if key.cmp(&tables[0].min_key()).is_lt() {
            return None;
        } else {
            for table in tables {
                if key.cmp(&table.min_key()).is_ge() && key.cmp(&table.max_key()).is_le() {
                    let mut iter = table.new_iterator();
                    if let Some(val) = iter.seek(key) {
                        return Some(val.clone());
                    }
                }
            }
        }
        None
    }
}

impl LevelHandler {
    pub fn add(&mut self, t: Table) {
        self.total_size += t.size();
        self.tables.push(t);
    }

    pub fn sort(&mut self) {
        if self.level_num == 0 {
            // key range will overlap, just sort by fileid in ascending order
            // because newer tables are at the end of level 0
            self.tables.sort_by(|lhs, rhs| {
                let lhs_id = lhs.id().unwrap();
                let rhs_id = rhs.id().unwrap();

                lhs_id.cmp(&rhs_id)
            });
        } else {
            self.tables
                .sort_by(|lhs, rhs| lhs.min_key().cmp(rhs.min_key()));
        }
    }
}
