use super::filter::Filter;
use super::slice::Slice;
use bloom::{BloomFilter, ASMS};

struct Filter_cargo {
    bloom: BloomFilter,
}

// impl Filter for Filter_cargo {
//     fn with_keys( keys: &[Slice]) ->Self{
//         let mut bloom = BloomFilter::with_rate(0.01, keys.len() as u32);

//         for key in keys {
//             bloom.insert(&||{
//                 let mut s = String::new();
//                 for &x in key{
//                     s.push(x as char);
//                 }
//                 s
//             });
//         }
//         Filter_cargo{
//             bloom
//         }
//     }

//     fn key_may_match(&self, key: &Slice) -> bool {
//         self.bloom.contains(&String::from(key))
//     }
// }
