use super::level::LevelManager;
use super::memtable::MemTable;
use super::options::Options;
use std::sync::Arc;
use tokio::task;

struct DB {
    mem_table: MemTable,
    immu_mem_tables: Vec<MemTable>,
    levels: Arc<LevelManager>,
    opt: Arc<Options>,
    max_mem_id: u64,
}

impl DB{
    async fn start_compacter(&self){
        let num = self.opt.num_compactors;
        for i in 0..num{
            let levels = self.levels.clone();
            tokio::spawn(async move{
                levels.run_compacter(i).await;
            });
        }
    }
}
