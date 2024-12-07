use crate::file::file;
use crate::utils::encodings::*;
use crate::utils::slice::Slice;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io;

fn estimate_entry_size(key: &[u8], val: &[u8]) -> u32 {
    key.len() as u32 + val.len() as u32 + 8
}

struct WalFile {
    f: MmapMut,
    size: usize,
    wrtie_at: usize,
}

impl WalFile {
    pub fn open(opt: file::Options) -> io::Result<WalFile> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(std::path::Path::new(&opt.dir).join(opt.file_name.clone()))?;
        file.set_len(opt.size)?;

        Ok(WalFile {
            f: unsafe { MmapMut::map_mut(&file)? },
            size: 0,
            wrtie_at: 0,
        })
    }

    pub fn size(&self) -> u32 {
        self.f.len() as u32
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        let mut v = Vec::new();
        v.append(&mut encode_varint_u32(key.len() as u32));
        v.append(&mut encode_varint_u32(val.len() as u32));
        v.extend_from_slice(key);
        v.extend_from_slice(val);

        self.f[self.wrtie_at..self.wrtie_at + v.len()].copy_from_slice(&v);
        self.wrtie_at += v.len();
    }
}

mod tests{
    use super::*;

    #[test]
    fn test_add(){
        let options = file::Options{
            size : 1024,
            file_name : "001.wal".to_string(),
            dir : "./workdir".to_string(),
            create: true,
        };

        let mut wal = WalFile::open(options).unwrap();
        wal.add("hello".as_bytes(), "world".as_bytes());
    }
}
