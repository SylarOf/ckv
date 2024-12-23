use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
pub struct Filter {
    filter: Vec<u8>,
}

impl Filter {
    pub fn new() -> Self {
        Filter { filter: Vec::new() }
    }

    pub fn with_filter(filter: &[u8]) ->Self{
        Filter{
            filter : filter.to_vec(),
        }
    }

    pub fn hash(mut key: &[u8]) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }

    pub fn bloom_bits_per_key(num_entries: i32, fp: f64) -> i32 {
        let size = -1.0 * (num_entries as f64) * fp.ln() / f64::powf(0.69314718056, 2.0);
        let locs = size / num_entries as f64;
        locs.ceil() as i32
    }

    pub fn with_keys(keys: &[u32], bits_per_key: i32) -> Self {
        Filter {
            filter: Self::append_filter(keys, bits_per_key),
        }
    }
    pub fn get(&self) -> Vec<u8> {
        self.filter.clone()
    }
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        Self::may_contain(self, Self::hash(key))
    }
    fn may_contain(&self, h: u32) -> bool {
        let mut h = h;
        if self.filter.len() < 2 {
            return false;
        }
        let k = self.filter[self.filter.len() - 1];
        if k > 30 {
            return true;
        }
        let n_bits = 8 * (self.filter.len() - 1) as u32;
        let delta = h >> 17 | h << 15;
        for _ in 0..k {
            let bit_pos = h % n_bits;
            if self.filter[bit_pos as usize / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            //h += delta;
            h = h.wrapping_add(delta);
        }
        true
    }
    fn append_filter(keys: &[u32], mut bits_per_key: i32) -> Vec<u8> {
        if bits_per_key < 0 {
            bits_per_key = 0;
        }

        let mut k = (bits_per_key as f64 * 0.69) as u32;

        if k < 1 {
            k = 1;
        }
        if k > 30 {
            k = 30;
        }

        let mut nbits = keys.len() as i32 * bits_per_key as i32;

        if nbits < 64 {
            nbits = 64;
        }
        let n_bytes = (nbits + 7) / 8;
        let n_bits = n_bytes * 8;
        let mut filter: Vec<u8> = vec![0; n_bytes as usize + 1];
        for &h in keys {
            let mut h = h;
            let delta = h >> 17 | h << 15;
            for _ in 0..k {
                let bitpos = h % (n_bits as u32);
                filter[(bitpos / 8) as usize] |= 1 << (bitpos % 8);
                //h += delta;
                h = h.wrapping_add(delta);
            }
        }

        filter[n_bytes as usize] = k as u8;

        filter
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;

    impl Filter {
        // Assuming the other methods like `hash`, `may_contain_key`, etc., are already implemented.

        // Function to convert the filter to a string representation
        pub fn to_string(&self) -> String {
            let mut s = String::new();
            for &byte in &self.filter {
                for i in 0..8 {
                    if byte & (1 << (7 - i)) != 0 {
                        s.push('1');
                    } else {
                        s.push('.');
                    }
                }
            }
            s
        }
    }
    // Test for the `may_contain_key` method
    #[test]
    fn test_bloom_filter_may_contain_key() {
        let keys = vec![
        Filter::hash(&"g".as_bytes().to_vec()),
        Filter::hash(&"go".as_bytes().to_vec())
        ];

        // Create the Bloom filter with keys and bits per key
        let filter = Filter::with_keys(&keys, 10);

        // Test for existing keys
        assert!(filter.may_contain_key(b"g"));
        assert!(filter.may_contain_key(b"go"));

        // Test for non-existing keys
        assert!(!filter.may_contain_key(b"x"));
        assert!(!filter.may_contain_key(b"foo"));
    }

    // Test for Bloom Filter size and false positives
    #[test]
    fn test_bloom_filter_size_and_false_positives() {
        // Simulate adding 1000 entries
        let keys = (0..1000).map(|i| i as u32).collect::<Vec<u32>>();

        // Create the filter with 10 bits per key
        let filter = Filter::with_keys(&keys, 10);

        // Check that the filter size is within an acceptable range (e.g., no excessive size)
        assert!(filter.filter.len() <= (1000 * 10) / 8 + 40);

        // Check for false positives: simulate checking for non-inserted keys
        let mut false_positives = 0;
        for i in 0..10000 {
            if filter.may_contain_key(&(i as u32).to_le_bytes()) {
                false_positives += 1;
            }
        }

        // Ensure the false positive rate is under a threshold (e.g., 2%)
        assert!(
            false_positives < 200,
            "False positives exceeded the threshold"
        );
    }

    // Test for Bloom filter's bits per key calculation
    #[test]
    fn test_bloom_bits_per_key() {
        // Test known values for bits per key calculation
        let num_entries = 1000;
        let fp = 0.01; // Desired false positive rate

        let bits_per_key = Filter::bloom_bits_per_key(num_entries, fp);

        // Assert the result is within expected range (based on the formula for bloom filter)
        assert!(bits_per_key > 0);
    }
}
