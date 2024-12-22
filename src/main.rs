mod db;
mod file;
mod pb;
mod table;
mod utils;

use std::sync::Arc;

use crate::db::db::DB;
use crate::db::options::Options;
use crate::utils::test_helper;

#[tokio::main]
async fn main() {
    let opt = Options::test_new();
    test_helper::work_dir_clear(&opt.work_dir).unwrap();
    let mut db = DB::open(Arc::new(opt)).unwrap();
    db.start_compacter().await;

    let n = 10000;
    let length = 8;
    for _ in 0..n {
        let key = test_helper::rand_str(length);
        let val = test_helper::rand_str(length);
        db.set(&key, &val).unwrap();
    }

    loop {}
}
