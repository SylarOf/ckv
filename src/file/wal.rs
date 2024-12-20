use crate::file::file;
use crate::utils::encodings::*;
use crate::utils::file::file_helper::fid_wal;
use crate::utils::slice::Slice;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io;

fn estimate_entry_size(key: &[u8], val: &[u8]) -> u32 {
    key.len() as u32 + val.len() as u32 + 8
}

pub struct WalFile {
    f: MmapMut,
    wrtie_at: usize,
    name: String,
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
            wrtie_at: 0,
            name: opt.file_name,
        })
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

    pub fn size(&self) -> u32 {
        self.wrtie_at as u32
    }

    pub fn id(&self) -> Result<u64, String> {
        fid_wal(&self.name)
    }

}

impl Iterator for WalFile {
    type Item = (Slice, Slice);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key_len, var_len)) = decode_varint_u32(&self.f[self.wrtie_at..]) {
            if key_len == 0 {
                return None;
            }
            self.wrtie_at += var_len;
            if let Some((val_len, var_len)) = decode_varint_u32(&self.f[self.wrtie_at..]) {
                self.wrtie_at += var_len;
                let key = Slice::from(&self.f[self.wrtie_at..self.wrtie_at + key_len as usize]);
                self.wrtie_at += key_len as usize;
                let val = Slice::from(&self.f[self.wrtie_at..self.wrtie_at + val_len as usize]);
                self.wrtie_at += val_len as usize;
                return Some((key, val));
            } else {
                return None;
            }
        }
        None
    }
}

mod tests {
    use super::*;
    use crate::utils::test_helper;
    #[test]
    fn test_add() {
        let options = file::Options {
            size: 1024,
            file_name: "001.wal".to_string(),
            dir: "./workdir".to_string(),
            create: true,
        };

        let mut wal = WalFile::open(options.clone()).unwrap();

        let keys = test_helper::generate_incredible_strings(10);

        for key in &keys {
            wal.add(key.as_bytes(), key.as_bytes());
        }
        let wal = WalFile::open(options).unwrap();

        let mut count = 0;
        for (key, val) in wal {
            assert_eq!(&key, &keys[count].as_bytes());
            assert_eq!(&val, &keys[count].as_bytes());
            count += 1;
        }
    }
}
