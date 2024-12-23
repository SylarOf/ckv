use crate::db::options::Options;
use crate::file::file;
use crate::file::wal::WalFile;
use crate::utils::file::file_helper::file_wal_name;
use crate::utils::slice::Slice;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
pub struct MemTable {
    pub(crate) skiplist: SkipMap<Slice, Slice>,
    wal: WalFile,
}

impl MemTable {
    pub fn new(opt: Arc<Options>) -> std::io::Result<Self> {
        opt.max_fid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let fid = opt.max_fid.load(std::sync::atomic::Ordering::Relaxed);
        let file_opt = file::Options {
            file_name: file_wal_name(fid),
            dir: opt.work_dir.clone(),
            size: opt.memtable_size,
            create: true,
        };
        let wal = WalFile::open(file_opt)?;

        Ok(MemTable {
            skiplist: SkipMap::new(),
            wal,
        })
    }

    pub fn open(opt: Arc<Options>, fid: u64) -> std::io::Result<Self> {
        let file_opt = file::Options {
            file_name: file_wal_name(fid),
            dir: opt.work_dir.clone(),
            size: opt.memtable_size,
            create: true,
        };
        let wal = WalFile::open(file_opt)?;
        let mut memtable = MemTable {
            skiplist: SkipMap::new(),
            wal,
        };
        memtable.replay();
        Ok(memtable)
    }

    pub fn insert(&mut self, key: &[u8], val: &[u8]) {
        // firstly write to wal file
        self.wal.add(key, val);

        // write to skiplist
        self.skiplist.insert(Slice::from(key), Slice::from(val));
    }

    pub fn seek(&self, key: &[u8]) -> Option<Slice> {
        self.skiplist.get(key).map(|v| v.value().clone())
    }

    pub fn size(&self) -> usize {
        self.wal.size() as usize
    }

    pub fn id(&self) -> Result<u64, String> {
        self.wal.id()
    }

    fn replay(&mut self) {
        for (key, val) in &mut self.wal {
            self.skiplist.insert(key, val);
        }
    }
}
