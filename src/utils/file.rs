use crc::{Crc, CRC_32_ISO_HDLC};

pub fn calculate_checksum(data: &[u8]) -> u64 {
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    crc32.checksum(data) as u64
}
pub fn calculate_checksum32(data: &[u8]) -> u32 {
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    crc32.checksum(data) as u32
}

pub fn verify_checksum(data: &[u8], checksum: &[u8]) -> bool {
    calculate_checksum(data).to_le_bytes() == checksum
}
pub fn verify_checksum_32(data: &[u8], checksum: &[u8]) -> bool {
    calculate_checksum32(data).to_le_bytes() == checksum
}


pub mod file_helper {

    use std::collections::HashSet;

    // use id to get wal file name
    pub fn file_wal_name(id: u64) -> String {
        let file_name = format!("{:05}.wal", id);
        file_name
    }
    // use id to get wal file name with dir
    pub fn file_wal_name_with_dir(dir: &str, id: u64) -> String {
        let file_name = format!("{:05}.wal", id);
        std::path::Path::new(&dir)
            .join(&file_name)
            .to_str()
            .unwrap()
            .to_string()
    }
    // use id to get sst file name
    pub fn file_sstable_name(id: u64) -> String {
        let file_name = format!("{:05}.sst", id);
        file_name
    }
    // use id to get sst file name with dir
    pub fn file_sstable_name_with_dir(dir: &str, id: u64) -> String {
        let file_name = format!("{:05}.sst", id);
        std::path::Path::new(&dir)
            .join(&file_name)
            .to_str()
            .unwrap()
            .to_string()
    }
    // use sst file name to get its fid
    pub fn fid(name: &str) -> Result<u64, String> {
        if !name.ends_with(".sst") {
            return Err("not a sst  file".to_string());
        }

        // remove the ".sst" suffix
        let name = name.trim_end_matches(".sst");

        match name.parse::<u64>() {
            Ok(id) => Ok(id),
            Err(e) => Err(e.to_string()),
        }
    }

    // load all sst file id in a dir
    pub fn load_id_set(dir: &str) -> std::io::Result<HashSet<u64>> {
        let mut set = HashSet::new();

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                continue;
            }

            let id = fid(entry.file_name().to_str().unwrap());
            if let Ok(id) = id {
                set.insert(id);
            }
        }
        Ok(set)
    }

    // use wal file name to get its fid
    pub fn fid_wal(name: &str) -> Result<u64, String> {
        if !name.ends_with(".wal") {
            return Err("not a wal  file".to_string());
        }

        // remove the ".wal" suffix
        let name = name.trim_end_matches(".wal");

        match name.parse::<u64>() {
            Ok(id) => Ok(id),
            Err(e) => Err(e.to_string()),
        }
    }
}
