use super::file::Options;
use crate::pb::*;
use crate::utils::file::file_helper;
use crate::utils::slice::Slice;
use memmap2::MmapMut;
use prost::Message;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

pub struct SSTable {
    name: String,
    f: MmapMut,
    max_key: Slice,
    min_key: Slice,
    has_filter: bool,
    table_index: pb::TableIndex,
    created_at: SystemTime,
}

impl SSTable {
    pub fn open(opt: Options) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(std::path::Path::new(&opt.dir).join(opt.file_name.clone()))?;

        if opt.create {
            file.set_len(opt.size).unwrap();
        }
        let metadata = file.metadata()?;
        Ok(SSTable {
            name: opt.file_name,
            f: unsafe { MmapMut::map_mut(&file)? },
            max_key: Slice::new(),
            min_key: Slice::new(),
            has_filter: true,
            table_index: pb::TableIndex::default(),
            created_at: SystemTime::now(),
        })
    }

    pub fn init(&mut self) -> Result<(), String> {
        let block_offset = self.init_table()?;
        self.min_key = block_offset.key.clone();
        Ok(())
    }

    pub fn read(&self, pos: u32, len: u32) -> &[u8] {
        &self.f[pos as usize..(pos + len) as usize]
    }
    pub fn offsets(&self, i: u32) -> Option<pb::BlockOffset> {
        if i as usize >= self.table_index.offsets.len() {
            return None;
        }
        Some(self.table_index.offsets[i as usize].clone())
    }

    // binary serach key in block
    pub fn seek(&self, key: &[u8]) -> Option<u32> {
        let found = self
            .indexs()
            .offsets
            .binary_search_by(|offset| offset.key.cmp(&Vec::from(key)));
        match found {
            Ok(idx) => Some(idx as u32),
            Err(idx) => {
                if idx >= 1 {
                    Some((idx - 1) as u32)
                } else {
                    None
                }
            }
        }
    }

    pub fn set_max_key(&mut self, max_key: Slice) {
        self.max_key = max_key;
    }
    pub fn write_table(&mut self, data: &[u8]) {
        let len = self.f.len();
        self.f[0..len].copy_from_slice(data);
    }

    pub fn id(&self) -> Result<u64, String> {
        file_helper::fid(&self.name)
    }
    pub fn indexs(&self) -> &pb::TableIndex {
        &self.table_index
    }

    pub fn max_key(&self) -> &Slice {
        &self.max_key
    }

    pub fn min_key(&self) -> &Slice {
        &self.min_key
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.has_filter
    }

    pub fn size(&self) -> u64 {
        self.f.len() as u64
    }

    pub fn delete(&mut self) -> io::Result<()> {
        std::fs::remove_file(self.name.clone())
    }

    pub fn get_create_at(&self) -> SystemTime {
        self.created_at
    }

    fn init_table(&mut self) -> Result<pb::BlockOffset, String> {
        let mut read_pos = self.f.len();
        let data = &self.f;

        //debug
        //println!("{:?}", &data[..]);
        // read checksum len from the last 4 bytes
        let buf = &data[read_pos - 4..read_pos];
        read_pos -= 4;
        let checksum_len = u32::from_le_bytes(buf.try_into().unwrap());

        // read checksum
        let checksum = &data[read_pos - checksum_len as usize..read_pos];
        read_pos -= checksum_len as usize;

        // read index size from the footer
        let buf = &data[read_pos - 4..read_pos];
        let idx_len = u32::from_le_bytes(buf.try_into().unwrap());
        read_pos -= 4;

        // read index
        let idx_data = &data[read_pos - idx_len as usize..read_pos];
        if crate::utils::file::verify_checksum(idx_data, checksum) == false {
            return Err(format!(
                "failed to verify checksum for table: {}",
                self.name
            ));
        }
        let table_index = pb::TableIndex::decode(idx_data).map_err(|e| e.to_string())?;
        self.table_index = table_index;
        self.has_filter = !self.table_index.bloom_filter.is_empty();
        if !self.table_index.offsets.is_empty() {
            return Ok(self.table_index.offsets[0].clone());
        }
        return Err("read index failed, offset is empty".to_string());
    }
}
