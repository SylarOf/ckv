#[derive(Default)]
pub struct Options {
    pub work_dir: String,
    pub memtable_size: u64,
    pub sstable_maxsz: u64,
    pub block_size: u64,
    pub bloom_false_positive: f64,

    pub num_compactors: u32,
    pub base_level_size: u64,
    pub level_size_multiplier: u32, // between level size expect ratio
    pub base_table_size: u64,
    pub table_size_multiplier: u32,
    pub num_level_zero_tables: u32,
    pub max_level_num: u32,
}

impl Options {
    pub fn test_new() -> Options {
        Options {
            work_dir: "./work_test".to_string(),
            memtable_size: 1024,
            sstable_maxsz: 1024,
            block_size: 1024,
            bloom_false_positive: 0.,
            num_compactors: 3,
            base_level_size: 10 << 20,
            level_size_multiplier: 10,
            base_table_size: 2 << 20,
            table_size_multiplier: 2,
            num_level_zero_tables: 15,
            max_level_num: 7,
        }
    }
}
