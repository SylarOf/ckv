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

