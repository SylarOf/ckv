use crate::db::options::Options;
use crate::utils::slice::Slice;
use crate::pb::pb::{BlockOffset, TableIndex};
use crate::file::sstable::SSTable;
use crate::file;
use crate::utils::filter::Filter;
use prost::Message;
use std::sync::Arc;
pub struct TableBuilder {
    opt: Arc<Options>,
    sst_size: u64,
    cur_block: Block,
    blocks: Vec<Block>,
    key_count: u32,
    key_hashs: Vec<u32>,
    base_key: Slice,
    estimate_size: i64,
}
#[derive(Default)]
struct BuildData{
    blocks : Vec<Block>,
    index : Vec<u8>,
    checksum: Vec<u8>,
    size : u32
}
#[derive(Clone)]
pub struct Block {
    offset: u32,
    entries_index_start: u32,
    data: Slice,
    base_key: Slice,
    entry_offsets: Vec<u32>,
    end: u32,
    estimate_sz: i64,
}

#[derive(Default)]
struct Header {
    overlap: u16,
    diff: u16,
}

impl Header {
    pub fn decode(header: &[u8]) -> Self {
        assert!(header.len() == 4);
        let overlap = u16::from_le_bytes([header[0], header[1]]);
        let diff = u16::from_le_bytes([header[2], header[3]]);
        Header { overlap, diff }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut header = Vec::with_capacity(4);
        header.extend_from_slice(&self.overlap.to_le_bytes());
        header.extend_from_slice(&self.diff.to_le_bytes());

        header
    }
}

impl Block {
    pub fn new(opt : Arc<Options>)->Self{
        let mut v = Vec::new();
        v.reserve(opt.block_size as usize);
        Block{
            offset: 0,
            entries_index_start: 0,
            data: v,
            base_key: Slice::new(),
            entry_offsets: Vec::new(),
            end: 0,
            estimate_sz: 0,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl TableBuilder {
    pub fn new(opt : Arc<Options>)->Self{
        TableBuilder{
            opt : opt.clone(),
            sst_size : opt.sstable_maxsz,
            cur_block: Block::new(opt.clone()),
            blocks: Vec::new(),
            key_count: 0,
            key_hashs: Vec::new(),
            base_key: Slice::new(),
            estimate_size: 0,
        }
    }
    pub fn add(&mut self, key: &[u8], value: &[u8]){
        if self.try_finish_block(key, value){
            self.finish_block();
            self.cur_block = Block::new(self.opt.clone())
        }
        self.key_hashs.push(Filter::hash(&key));

        let mut diffkey;

        if self.cur_block.base_key.len() == 0 {
            self.cur_block.base_key = Slice::from(key);
            diffkey = Slice::from(key);
        }
        else {
            diffkey = self.key_diff(key);
        }
        let header = Header{
            overlap : (key.len() - diffkey.len()) as u16,
            diff : diffkey.len() as u16,
        };
        self.cur_block.entry_offsets.push(self.cur_block.end);
        self.append(&mut header.encode());
        self.append(&mut diffkey);
        self.append(&mut Slice::from(value));
    }

    pub fn is_empty(&self)->bool{
        self.key_hashs.is_empty() 
    }
    fn try_finish_block(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!((self.cur_block.entry_offsets.len() as u32 + 1) * 4 + 4 + 8 + 4 < u32::MAX);
        let entries_offsets_size = (self.cur_block.entry_offsets.len() + 1) * 4 
        + 4 // size of blocks
        + 8 // sum64 in checksum proto
        + 4 // checksum length
        ;
        self.cur_block.estimate_sz = self.cur_block.end as i64
            + 4 
            + key.len() as i64
            + value.len() as i64
            + entries_offsets_size as i64;

         self.cur_block.estimate_sz > self.opt.block_size as i64
    }
    fn append(&mut self, data : &mut Vec<u8>){
        self.allocate(data.len() as i32);
        self.cur_block.end+=data.len() as u32;

        // debug
        //println!("now block end: {}", self.cur_block.end);
        self.cur_block.data.append(data);
    }
    fn allocate(&mut self, need : i32){
        let mut b = &mut self.cur_block;
        if (b.data.len() as i32) - (b.end as i32) < need{
            let mut sz = 2*b.data.len() as i32;
            if b.end as i32 + need > sz as i32{
                sz = b.end as i32 + need;
            }

            let mut tmp = Vec::new();
            tmp.reserve(sz as usize);
            tmp.append(&mut b.data);
            b.data = tmp;
        }
    }
    fn finish_block(&mut self){
        let mut v = Vec::new();
        // put entry_offsets
        for &num in &self.cur_block.entry_offsets{
            v.extend_from_slice(&num.to_le_bytes());
        }
        // put entry_offsets_len
        v.extend_from_slice(&(self.cur_block.entry_offsets.len() as u32).to_le_bytes());
        self.append(&mut v);
        v.clear();
        let cs = crate::utils::file::calculate_checksum(&self.cur_block.data[..self.cur_block.end as usize]).to_le_bytes();
        // put checksum
        v.extend_from_slice(&cs);
        // put checksum_len
        v.extend_from_slice(&(cs.len()as u32).to_le_bytes());

        self.append(&mut v);

        self.estimate_size += self.cur_block.estimate_sz;
        self.key_count += self.cur_block.entry_offsets.len() as u32;
        self.blocks.push(self.cur_block.clone());
    }
    
    fn key_diff(&self,key: &[u8])->Slice{
        let mut i = 0;
        while i < key.len() && i < self.cur_block.base_key.len() {
            if key[i] != self.cur_block.base_key[i]{
                break;
            }
            i+=1;
        }
        Vec::from(&key[i..])
    }

    pub fn flush(&mut self,name : String) ->std::io::Result<SSTable>{
        
        let build_data = self.done();
        let options = file::file::Options{
            size : build_data.size as u64,
            file_name : name,
            dir : self.opt.work_dir.clone(),
            create : true,
        };
        let mut ss = SSTable::open(options)?;
        ss.write_table(&build_data.copy()); 
        Ok(ss)
    }

    pub fn reach_capacity(&self) ->bool{
        self.estimate_size as u64 > self.opt.sstable_maxsz
    }

    // note: can't move a part under &mut 
    fn done(&mut self) ->BuildData{
        self.finish_block();
        if self.blocks.len() == 0{
            return BuildData::default();
        }
        let mut f;
        let mut bd = BuildData::default();
        bd.blocks = self.blocks.clone();
        if self.opt.bloom_false_positive > 0.0{

            let bits = Filter::bloom_bits_per_key(self.key_hashs.len() as i32, self.opt.bloom_false_positive);
            f = Filter::with_keys(&self.key_hashs, bits).get();
        }
        else {f = Vec::new();}
        let (index, data_size) = self.build_index(f);
        let checksum = crate::utils::file::calculate_checksum(&index).to_le_bytes().to_vec();

        bd.index = index;
        bd.checksum = checksum;
        // data + index + checksum + index.len + checksum.len
        bd.size = data_size + bd.index.len() as u32 + bd.checksum.len() as u32 + 4 + 4;
        
        // debug!
        //println!("bd.size : {}, data size: {}, index size {}",bd.size, data_size, bd.index.len());
        bd

    }
    fn build_index(&mut self, bloom : Vec<u8>)->(Vec<u8>, u32){
        let mut table_index = TableIndex::default();
        if bloom.len() > 0{
            table_index.bloom_filter = bloom.clone();
        }
        table_index.key_count = self.key_count;
        table_index.offsets = self.write_block_offsets(&mut table_index);
        let mut data_size = 0;
        for x in &self.blocks{
            data_size += x.end;
        } 
        let mut data = table_index.encode_to_vec();
        (data, data_size)
    }

    fn write_block_offsets(&self,table_index : &mut TableIndex)->Vec<BlockOffset>{
        let mut v = Vec::new();
        let mut start_offset = 0;
        for block in &self.blocks{
            v.push(Self::write_block_offset(block, start_offset));
            start_offset += block.end;
        }
        v
    }
    fn write_block_offset(bl : &Block, start_offset : u32) -> BlockOffset{
        BlockOffset{
            key : bl.base_key.clone(),
            len : bl.end,
            offset : start_offset,
        }
    }

    

}

impl BuildData{
    pub fn copy(&self)->Vec<u8>{
        let mut v = Vec::new();
        for block in &self.blocks{
            v.extend_from_slice(&block.data[..block.end as usize]);
        }
        let data_size = v.len();
        v.extend_from_slice(&self.index);
        let index_size = v.len() - data_size;
        v.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        //debug
        //println!("{}", v.len());
        v.extend_from_slice(&self.checksum);
        //println!("{}", v.len());
        v.extend_from_slice(&(self.checksum.len() as u32).to_le_bytes());
        //println!("{}", v.len());

        // debug!
        //println!("slice size : {}, data size : {}, index size : {}", v.len(), data_size,index_size);
        v
    }
}

#[derive(Default)]
pub struct BlockIterator<'a>{
    data : &'a [u8],
    idx : i32,
    base_key : Slice,
    key : Slice,
    val: Slice,
    entry_offsets : Vec<u32>,
    
}

impl <'a>BlockIterator<'a> {
    pub fn new(data : &'a [u8], base_key : &[u8]) ->Self{
        BlockIterator{
            data,
            idx : 0,
            base_key : Slice::from(base_key),
            key : Slice::new(),
            val : Slice::new(), 
            entry_offsets : Vec::new(),
        } 
    } 
    pub fn seek_to_first(&mut self){
        self.set_idx(0);
    }
    
    pub fn seek_to_last(&mut self){
        self.set_idx(self.entry_offsets.len() as i32 - 1);
    }

    pub fn next(&mut self)->Option<()>{
        let idx = self.idx +1;
        if idx < 0 || idx >= self.entry_offsets.len() as i32{
            return None;
        }
        self.set_idx(self.idx+1);
        Some(())
    }
    // todo! to ref but not to clone entry_offsets
    pub fn seek(&mut self, key : &[u8])->Option<&Slice>{
        let key_len = self.entry_offsets.len();
        let mut seek_arr = Vec::new();
        for i in 0..key_len{
            seek_arr.push(i);
        }
        let found_idx = seek_arr.binary_search_by(|&i|{
            //println!("i is {}", i);
            self.set_idx(i as i32);
            self.key.cmp(&Vec::from(key))
        });


        if let Ok(i) = found_idx{
            self.set_idx(i as i32);
            Some(&self.key)
        } 
        else{
            None
        }

    } 

    pub fn valid(&self, idx : i32) ->bool{
        idx >= 0 && idx < self.entry_offsets.len() as i32
    }
    pub fn key(&self) ->&Slice{
        &self.key
    }
    pub fn val(&self) ->&Slice{
        &self.val
    }
    pub fn init(&mut self)->Result<(), String>{
        let data = self.data;
        let mut read_pos = data.len();

        //println!("data len : {}",data.len());
        
        // read checksum_len
        let checksum_len = u32::from_le_bytes(data[read_pos-4..read_pos].try_into().unwrap());
        read_pos -=4;

        //println!("checksum len:{}",checksum_len);

        // read checksum
        let checksum = &data[read_pos-checksum_len as usize..read_pos as usize];
        read_pos -=checksum_len as usize;

        if crate::utils::file::verify_checksum(&data[..read_pos], checksum) == false{
            return Err("verify checksum failed".to_string());
        }

        // read len of entry_offsets
        let num_entries = u32::from_le_bytes(data[read_pos-4..read_pos].try_into().unwrap());
        read_pos -=4;


        //debug
        //println!("num entries : {}", num_entries);
        // read entry_offsets
        let entry_offsets = &data[read_pos-num_entries as usize*4.. read_pos];
        for i in 0..num_entries as usize{
            let offset = u32::from_le_bytes(entry_offsets[i*4 .. i*4 + 4].try_into().unwrap());
            self.entry_offsets.push(offset);
        }
        read_pos -= num_entries as usize*4;

        self.data = &data[0..read_pos];

        Ok(())
        
    }
    fn set_idx(&mut self,i : i32){

        //debug
        //println!("now idx in block iter, {}", i);
        self.idx = i;
        assert!(i>=0 && i < self.entry_offsets.len() as i32);
        let start_offset = self.entry_offsets[i as usize];
        let end_offset;
        if i as usize + 1 == self.entry_offsets.len(){
            end_offset = self.data.len() as u32;
        } 
        else {
            end_offset = self.entry_offsets[i as usize +1];
        }
        let entry = &self.data[start_offset as usize..end_offset as usize];

        //debug
        //println!("entry : {:?}",entry);
        let header = Header::decode(&entry[0..4]);
        let mut key = Slice::new();
        key.extend_from_slice(&self.base_key[0..header.overlap as usize]); 
        key.extend_from_slice(&entry[4..4+header.diff as usize]);
        self.key = key;
        self.val = Slice::from(&entry[4+header.diff as usize..]);
    }
    
}

