use super::options::Options;
use crate::file::manifest::ManifestFile;
use crate::table::table::Table;
use std::sync::{atomic::AtomicU64, Arc, RwLock};
struct LevelManager {
    max_fid: AtomicU64,
    opt: Arc<Options>,
    manifest_file: ManifestFile,
    levels: Vec<RwLock<LevelHandler>>,
}

#[derive(Default)]
struct LevelHandler {
    level: u32,
    tables: Vec<Table>,
    total_size: u64,
}

impl LevelManager {
    pub fn new(opt: Arc<Options>) -> Result<LevelManager, String> {
        let manifest_file = ManifestFile::open(opt.clone())
            .map_err(|e| format!("failed to open the manifest ,{}", e))?;

        manifest_file.revert(set)
        let levels = (0..opt.max_level_num)
            .map(|_| RwLock::new(LevelHandler::default()))
            .collect();
        Ok(LevelManager {
            max_fid: 0.into(),
            opt,
            manifest_file,
            levels,
        })
    }

    pub fn build(&mut self) {}
}
