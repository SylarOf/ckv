use super::options::Options;
use crate::file::manifest::ManifestFile;
use crate::table::table::Table;
use std::sync::{Arc, RwLock};
struct LevelManager {
    max_fid: u64,
    opt: Arc<Options>,
    manifest_file: ManifestFile,
    levels: Vec<RwLock<LevelHandler>>,
}

struct LevelHandler {
    level: u32,
    tables: Vec<Table>,
    total_size: u64,
}
