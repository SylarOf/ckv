use crate::db::iterator::DBIterator;
use crate::db::options::Options;
use crate::file::file;
use crate::file::sstable::SSTable;
use crate::table::table_builder::{BlockIterator, TableBuilder};
use crate::utils::slice::Slice;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::SystemTime;

pub struct Table {
    sstable: SSTable,
    ref_count: AtomicU32,
    //opt: Arc<Options>,
}

impl Table {
    pub fn open(
        opt: Arc<Options>,
        name: String,
        table_builder: Option<TableBuilder>,
    ) -> std::io::Result<Table> {
        let mut table;
        if let Some(mut builder) = table_builder {
            table = builder.flush(name.clone())?;
        } else {
            let file_options = file::Options {
                size: opt.sstable_maxsz,
                file_name: name.clone(),
                dir: opt.work_dir.clone(),
                create: false,
            };
            table = SSTable::open(file_options)?;
        }

        table.init().unwrap();
        Ok(Table {
            sstable: table,
            ref_count: AtomicU32::new(1),
        })
    }

    // incre ref count of table
    pub fn incr_ref(&self) {
        self.ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // decre ref count of table, if count is 0, delete file
    pub fn decr_ref(&self) -> std::io::Result<()> {
        let ref_count = self
            .ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        if ref_count == 0 {
            self.sstable.delete()?
        }
        Ok(())
    }

    pub fn new_iterator(&self) -> TableIterator {
        TableIterator {
            table: &self,
            block_pos: 0,
            bi: BlockIterator::default(),
        }
    }

    pub fn id(&self) -> Result<u64, String> {
        self.sstable.id()
    }
    pub fn size(&self) -> u64 {
        self.sstable.size()
    }
    pub fn min_key(&self) -> &Slice {
        self.sstable.min_key()
    }
    pub fn max_key(&self) -> &Slice {
        self.sstable.max_key()
    }
    pub fn create_at(&self) -> SystemTime {
        self.sstable.get_create_at()
    }
}

pub struct TableIterator<'a> {
    table: &'a Table,
    block_pos: u32,
    bi: BlockIterator<'a>,
}

impl<'a> TableIterator<'a> {
    pub fn seek_to_first(&mut self) {
        self.set_block(0);
        self.bi.seek_to_first();
    }

    pub fn key(&self) -> &Slice {
        self.bi.key()
    }

    pub fn val(&self) -> &Slice {
        self.bi.val()
    }

    pub fn next(&mut self) -> Option<()> {
        match self.bi.next() {
            Some(()) => return Some(()),
            None => {
                self.set_block(self.block_pos + 1)?;
                self.bi.seek_to_first();
                return Some(());
            }
        }
    }

    pub fn seek(&mut self, key: &[u8]) -> Option<&Slice> {
        let block_idx = self.table.sstable.seek(key)?;
        self.set_block(block_idx)?;
        self.bi.seek(key)
    }

    fn set_block(&mut self, idx: u32) -> Option<()> {
        let offsets = self.table.sstable.offsets(idx)?;

        let data = self.table.sstable.read(offsets.offset, offsets.len);

        //debug
        //println!("{:?}", data);
        let base_key = offsets.key;

        self.bi = BlockIterator::new(data, &base_key);
        self.bi.init().unwrap();

        self.block_pos = idx;
        Some(())
    }
}

impl<'a> DBIterator for TableIterator<'a> {
    fn seek_to_first(&mut self) {
        self.seek_to_first();
    }

    fn seek(&mut self, key: &Slice) -> Option<&Slice> {
        self.seek(key)
    }

    fn next(&mut self) -> Option<()> {
        self.next()
    }

    fn key(&self) -> &Slice {
        self.key()
    }

    fn val(&self) -> &Slice {
        self.val()
    }
}

mod tests {
    use super::*;
    use crate::utils::test_helper;
    #[test]
    fn test_table() {
        let option = Options::test_new();
        let option = Arc::new(option);

        let mut table_builder = TableBuilder::new(option.clone());

        let num = 1000;
        let keys = test_helper::generate_incredible_strings(num);

        for key in keys {
            table_builder.add(&key.as_bytes(), &key.as_bytes());
            println!("key : {}, value : {}", key, key);
        }

        let sstable = table_builder.flush("001".to_string()).unwrap();

        let table = Table::open(option.clone(), "001".to_string(), None).unwrap();

        let mut iter = table.new_iterator();
        iter.seek_to_first();

        let value = iter.seek("abc".as_bytes());
        println!(
            "key : {}, value : {}",
            test_helper::display(iter.key()).unwrap(),
            test_helper::display(iter.val()).unwrap()
        );

        while let Some(()) = iter.next() {
            println!(
                "key : {}, value : {}",
                test_helper::display(iter.key()).unwrap(),
                test_helper::display(iter.val()).unwrap()
            );
        }
        let value = iter.seek("abj".as_bytes());
        println!("{}", test_helper::display(&value.unwrap()).unwrap());
        println!(
            "key : {}, value : {}",
            test_helper::display(iter.key()).unwrap(),
            test_helper::display(iter.val()).unwrap()
        );
    }
}
