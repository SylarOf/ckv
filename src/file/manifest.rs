use crate::db::options::Options;
use crate::file::file;
use crate::pb::pb;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

pub struct ManifestFile {
    f: Mutex<File>,
    manifest: Manifest,
    opt: Arc<Options>,
}

struct Manifest {
    levels: Vec<LevelManifest>,
    tables: HashMap<u64, TableManifest>,
    creations: u32,
    deletions: u32,
}

// levelManifest storage tables per level
type LevelManifest = HashSet<u64>;

// use TableManifest to get level of table with table id
struct TableManifest {
    level: u8,
    checksum: Vec<u8>,
}

impl ManifestFile {
    pub fn open(opt: Arc<Options>) -> std::io::Result<ManifestFile> {
        let manifest_path = std::path::Path::new(&opt.work_dir).join(file::MANIFSET_NAME);
        let res = File::open(manifest_path);
        let mut file;
        let num;
        if let Err(e) = res {
            match e.kind() {
                std::io::ErrorKind::NotFound => {
                    let m = Manifest::new();
                    let (f, n) = Self::help_rwrite(&opt.work_dir, &m)?;
                    file = f;
                    num = n;
                }
                std::io::ErrorKind::Other => {
                    println!("failed to open the file : {}", e);
                    return Err(e);
                }
                _ => {
                    return Err(e);
                }
            }
        } else {
            file = res.unwrap();
        }

        // if open, replay the manifest
        let manifest = Manifest::with_file(&mut file).unwrap();
        Ok(ManifestFile {
            f: Mutex::new(file),
            manifest,
            opt,
        })
    }

    pub fn add_changes(&mut self, cs: Vec<pb::ManifestChange>) -> Result<(), String> {
        let cs = pb::ManifestChangeSet { changes: cs };
        let mut buf = cs.encode_to_vec();
        let mut manifest_file = self.f.lock().unwrap();
        self.manifest.apply_change_set(cs)?;

        let mut v = Vec::new();
        v.extend_from_slice(&buf.len().to_le_bytes());
        let crc32 = crate::utils::file::calculate_checksum32(&buf);
        v.extend_from_slice(&crc32.to_le_bytes());

        v.append(&mut buf);
        manifest_file.write_all(&v).unwrap();
        Ok(())
    }

    pub fn revert(&self, set: HashSet<u64>) -> Result<(), String> {
        // set : file exists
        for (fid, _) in &self.manifest.tables {
            if set.contains(&fid) == false {
                return Err(format!("file does not exist for table {}", fid));
            }
        }
        for fid in set {
            if self.manifest.tables.contains_key(&fid) == false {
                let filename = file::file_sstable_name(&self.opt.work_dir, fid);
                if let Err(e) = std::fs::remove_file(filename) {
                    return Err(format!("remove file error, {}", e));
                }
            }
        }
        Ok(())
    }

    fn help_rwrite(dir: &String, m: &Manifest) -> std::io::Result<(File, u32)> {
        let rewrite_path = std::path::Path::new(&dir).join(file::MANIFEST_REWRITE_NAME);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            // appen == true, set the cursor always to end
            .append(true)
            .open(&rewrite_path)?;

        let mut buf = Vec::new();

        let magic_text = file::MAGIC_TEXT;
        let magic_version = file::MAGIC_VERSION.to_le_bytes();
        buf.extend_from_slice(magic_text);
        buf.extend_from_slice(&magic_version);

        let num_creations = m.tables.len();
        let changes = m.as_changes();
        let changes_len = changes.len();
        let c_set = pb::ManifestChangeSet { changes };

        let changes_buf = c_set.encode_to_vec();
        buf.extend_from_slice(&changes_len.to_le_bytes());
        let checksum = crate::utils::file::calculate_checksum32(&changes_buf);
        buf.extend_from_slice(&checksum.to_be_bytes());
        buf.extend_from_slice(&changes_buf);

        file.write_all(&buf)?;
        file.sync_all();

        let manifest_path = std::path::Path::new(&dir).join(file::MANIFSET_NAME);
        std::fs::rename(rewrite_path, &manifest_path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&manifest_path)?;

        Ok((file, num_creations as u32))
    }
}

impl Manifest {
    pub fn new() -> Manifest {
        Manifest {
            levels: Vec::new(),
            tables: HashMap::new(),
            creations: 0,
            deletions: 0,
        }
    }

    // replay_with_file apply all the changes in existed manifest file
    pub fn with_file(file: &mut File) -> Result<Manifest, String> {
        let mut magic_buf = [0u8; 8];
        file.read_exact(&mut magic_buf).unwrap();
        if &magic_buf[0..4] != file::MAGIC_TEXT
            || &magic_buf[4..8] != file::MAGIC_VERSION.to_le_bytes()
        {
            return Err("magic not equal".to_string());
        };

        let mut manifest = Manifest::new();
        let mut crc_buf = [0u8; 8];
        file.read_exact(&mut crc_buf).unwrap();

        let data_len = u32::from_le_bytes(crc_buf[0..4].try_into().unwrap());
        let crc = &crc_buf[4..8];

        let mut data_buf = vec![0u8; data_len as usize];
        file.read_exact(&mut data_buf);

        if crate::utils::file::verify_checksum(&data_buf, crc) == false {
            return Err("checksum not equal".to_string());
        }
        let change_set = pb::ManifestChangeSet::decode(&data_buf[..]).unwrap();

        manifest.apply_change_set(change_set).unwrap();

        Ok(manifest)
    }

    fn apply_change_set(&mut self, cs: pb::ManifestChangeSet) -> Result<(), String> {
        for c in cs.changes {
            self.apply_change(c)?
        }
        Ok(())
    }
    fn apply_change(&mut self, c: pb::ManifestChange) -> Result<(), String> {
        if c.op() == pb::manifest_change::Operation::Create {
            if self.tables.contains_key(&c.id) {
                return Err(format!("manifest invalid, table {} exists", c.id));
            }
            let table_manifest = TableManifest {
                level: c.level as u8,
                checksum: c.checksum,
            };

            self.tables.insert(c.id, table_manifest);

            // if previous level is empty, insert empty LevelManifest
            while self.levels.len() <= c.level as usize {
                self.levels.push(LevelManifest::new());
            }
            self.levels[c.level as usize].insert(c.id);
            self.creations += 1;
        } else {
            if self.tables.contains_key(&c.id) == false {
                return Err(format!("manifest removes non-existing table {}", c.id));
            }
            self.levels[c.level as usize].remove(&c.id);
            self.tables.remove(&c.id);
        }
        Ok(())
    }

    // convert manifest file to changes
    fn as_changes(&self) -> Vec<pb::ManifestChange> {
        let mut res = Vec::new();
        for (id, tm) in &self.tables {
            let change = Self::new_create_change(id, &(tm.level as u32), &tm.checksum);
            res.push(change);
        }
        res
    }

    // create a manifest change
    fn new_create_change(id: &u64, level: &u32, checksum: &Vec<u8>) -> pb::ManifestChange {
        pb::ManifestChange {
            id: *id,
            op: pb::manifest_change::Operation::Create as i32,
            level: *level,
            checksum: checksum.clone(),
        }
    }
}
