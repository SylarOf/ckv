use crate::utils::encodings::*;
use std::convert::From;
use std::ops::Index;
pub type Slice = Vec<u8>;


// pub struct Slice {
//     slice: Vec<u8>,
// }

// impl Slice {
//     pub fn new() -> Self {
//         Slice { slice: Vec::new() }
//     }
//     pub fn len(&self) -> u32 {
//         self.slice.len() as u32
//     }
//     pub fn with_vec(v: Vec<u8>) -> Self {
//         Slice { slice: v }
//     }
//     pub fn append(&mut self, src: &mut Slice) {
//         self.slice.append(&mut src.slice);
//     }
//     pub fn append_index(&mut self, src: &Slice, index: u32) {
//         for x in index..src.len() {
//             self.slice.push(src[x]);
//         }
//     }
//     pub fn assign_len(&mut self, src: &Slice, len: u32) {
//         for i in 0..=len {
//             self.slice.push(src[i]);
//         }
//     }
//     pub fn is_empty(&self) -> bool {
//         self.slice.is_empty()
//     }
// }

// impl Index<u32> for Slice {
//     type Output = u8;

//     fn index(&self, index: u32) -> &Self::Output {
//         &self.slice[index as usize]
//     }
// }

// impl From<String> for Slice {
//     fn from(value: String) -> Self {
//         let v = value.as_bytes().into_iter().copied().collect();
//         Slice { slice: v }
//     }
// }

// impl From<&Slice> for String {
//     fn from(value: &Slice) -> Self {
//         let mut res = String::new();
//         for &x in &value.slice {
//             res.push(x as char);
//         }
//         res
//     }
// }
// impl Slice {
//     pub fn put_fixed_32(&mut self, value: u32) {
//         let mut buf = encode_fixed_u32(value);
//         self.slice.append(&mut buf);
//     }

//     pub fn put_fixed_64(&mut self, value: u64) {
//         let mut buf = encode_fixed_u64(value);
//         self.slice.append(&mut buf);
//     }

//     pub fn put_varint_32(&mut self, value: u32) {
//         let mut buf = encode_varint_u32(value);
//         self.slice.append(&mut buf);
//     }

//     pub fn put_varint_64(&mut self, value: u64) {
//         let mut buf = encode_varint_u64(value);
//         self.slice.append(&mut buf);
//     }

//     pub fn decode_fixed_32(&self, index: u32) -> u32 {
//         assert!(self.len() >= 4);
//         decode_fixed_32(&self.slice[index as usize..index as usize + 4])
//     }

//     pub fn decode_fixed_64(&self, index: u32) -> u64 {
//         assert!(self.len() >= 8);
//         let index = index as usize;
//         decode_fixed_64(&self.slice[index as usize..index as usize + 8])
//     }

//     pub fn decode_varint_u32(&self, index: u32) -> (u32, u32) {
//         decode_varint_u32(&self.slice[index as usize..])
//     }
//     pub fn decode_varint_u64(&self, index: u32) -> (u64, u32) {
//         decode_varint_u64(&self.slice[index as usize..])
//     }
// }
