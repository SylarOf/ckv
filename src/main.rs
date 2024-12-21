mod db;
mod pb;
mod table;
mod utils;
mod file;

use std::sync::Arc;

use crate::db::options::Options;
use crate::utils::test_helper;
use crate::db::db::DB;

#[tokio::main]
async fn main() {
        let opt = Options::test_new();
        test_helper::work_dir_clear(&opt.work_dir).unwrap();
        let mut db = DB::open(Arc::new(opt)).unwrap();
        db.start_compacter().await;

        let v = test_helper::generate_incredible_strings(3000);
        for x in &v {
            db.set("hello", "world").unwrap();
        }

        loop {}


}
