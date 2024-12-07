use std::collections::hash_set;
pub struct Options {
    pub file_name: String,
    pub dir: String,
    pub size: u64,
    pub create: bool,
}

pub const MANIFSET_NAME: &str = "MANIFEST";
pub const MANIFEST_REWRITE_NAME: &str = "REWRITEMANIFEST";

pub const MAGIC_TEXT: &[u8] = "bupt".as_bytes();
pub const MAGIC_VERSION: u32 = 1;

pub fn file_sstable_name(dir: &str, id: u64) -> String {
    let file_name = format!("{:05}.sst", id);
    std::path::Path::new(&dir)
        .join(&file_name)
        .to_str()
        .unwrap()
        .to_string()
}

pub fn load_id_set(dir: &str) ->