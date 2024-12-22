use super::level::LevelManager;
use super::memtable::MemTable;
use super::options::Options;
use crate::file::manifest::TableMeta;
use crate::table::table::Table;
use crate::table::table_builder::TableBuilder;
use crate::utils::file::file_helper;
use crate::utils::slice::Slice;
use std::sync::Arc;

pub(crate) struct DB {
    mem_table: Option<MemTable>,
    immu_mem_tables: Vec<MemTable>,
    levels: Arc<LevelManager>,
    opt: Arc<Options>,
}

impl DB {
    pub fn open(opt: Arc<Options>) -> Result<Self, String> {
        let level_manager = Arc::new(LevelManager::new(opt.clone())?);
        let mut db = DB {
            mem_table: None,
            immu_mem_tables: Vec::new(),
            levels: level_manager,
            opt,
        };

        db.recovery().map_err(|e| e.to_string())?;
        Ok(db)
    }

    pub fn set<T: AsRef<str>>(&mut self, key: T, val: T) -> Result<(), String> {
        let key = key.as_ref().as_bytes();
        let val = val.as_ref().as_bytes();
        // check if memtable is full

        if let Some(mem_table) = &self.mem_table {
            if mem_table.size() + key.len() + val.len() + 5 + 5 > self.opt.memtable_size as usize {
                self.immu_mem_tables.push(self.mem_table.take().unwrap());
                self.mem_table = Some(MemTable::new(self.opt.clone()).map_err(|e| e.to_string())?);
            }
        }
        if let Some(mem_table) = &mut self.mem_table {
            mem_table.insert(key, val);
        }

        if self.immu_mem_tables.is_empty() == false {
            let immu_mem_tables = std::mem::replace(&mut self.immu_mem_tables, Vec::new());
            for immu_mem_table in immu_mem_tables {
                let wal_name =
                    file_helper::file_wal_name_with_dir(&self.opt.work_dir, immu_mem_table.id()?);
                self.flush_memtable(immu_mem_table)?;
                std::fs::remove_file(wal_name).map_err(|e| e.to_string())?;
            }
        }

        Ok(())
    }

    pub fn get<T: AsRef<str>>(&self, key: T) -> Option<Slice> {
        let key = key.as_ref().as_bytes();

        if let Some(mem_table) = &self.mem_table {
            if let Some(val) = mem_table.seek(key) {
                return Some(val);
            }
        }

        for immu_mem_table in &self.immu_mem_tables {
            if let Some(val) = immu_mem_table.seek(key) {
                return Some(val);
            }
        }

        self.levels.get(key)
    }

    // debug!
    // pub for debug
    pub async fn start_compacter(&self) {
        let num = self.opt.num_compactors;

        for i in 0..num {
            let levels = self.levels.clone();
            tokio::spawn(async move {
                levels.run_compacter(i).await;
            });
        }
    }

    fn recovery(&mut self) -> std::io::Result<()> {
        let mut fids = Vec::new();
        for entry in std::fs::read_dir(&self.opt.work_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                continue;
            }

            if let Ok(id) = file_helper::fid_wal(entry.file_name().to_str().unwrap()) {
                // consider the existence of the wal file and update max_fid
                let max_fid = self.opt.max_fid.load(std::sync::atomic::Ordering::Relaxed);
                if max_fid < id {
                    self.opt
                        .max_fid
                        .store(id, std::sync::atomic::Ordering::Relaxed);
                }
                fids.push(id);
            }
        }
        fids.sort();

        for fid in fids {
            let mem = MemTable::open(self.opt.clone(), fid)?;
            self.immu_mem_tables.push(mem);
        }

        self.mem_table = Some(MemTable::new(self.opt.clone())?);

        Ok(())
    }

    fn flush_memtable(&mut self, immu_mem_table: MemTable) -> Result<(), String> {
        // alloc a fid
        let fid = immu_mem_table.id()?;
        let sst_name = file_helper::file_sstable_name(fid);

        let mut table_builder = TableBuilder::new(self.opt.clone());
        for entry in immu_mem_table.skiplist.iter() {
            let (key, val) = (entry.key(), entry.value());

            table_builder.add(key, val);
        }

        // create a table

        let table = Table::open(self.opt.clone(), sst_name, Some(table_builder))
            .map_err(|e| e.to_string())?;
        let mut manifest_file = self.levels.manifest_file.write().unwrap();

        manifest_file.add_table_meta(
            0,
            TableMeta {
                id: fid,
                checksum: "bupt".as_bytes().to_vec(),
            },
        )?;

        self.levels.levels[0].write().unwrap().add(table);

        Ok(())
    }
}

mod tests {

    use super::*;
    use crate::utils::test_helper;
    #[tokio::test]
    async fn test_db_start() {
        let opt = Options::test_new();
        test_helper::work_dir_clear(&opt.work_dir).unwrap();
        let mut db = DB::open(Arc::new(opt)).unwrap();
        db.start_compacter().await;

        let v = test_helper::generate_incredible_strings(1000);
        for x in &v {
            db.set(x, x).unwrap();
        }

        loop {}
    }
}
