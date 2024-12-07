// if you get a vec<u8> of encode, it wll previously write and copy
// directly encode to src

// if not return ptr, consider using **ptr
pub fn encode_varint_u32_ptr(mut ptr: *mut u8, mut value: u32) -> *mut u8 {
    let b = 128;
    unsafe {
        while value >= b {
            *ptr = ((value & (b - 1)) | b) as u8;
            value >>= 7;
            ptr = ptr.add(1);
        }
        *ptr = value as u8;
        ptr = ptr.add(1);
    }
    ptr
}

pub fn encode_varint_u64_ptr(mut ptr: *mut u8, mut value: u64) -> *mut u8 {
    //if first bit is zero, terminal
    let b = 128;
    unsafe {
        while value >= b {
            *ptr = ((value & (b - 1)) | b) as u8;
            value >>= 7;
            ptr = ptr.add(1);
        }
        *ptr = value as u8;
        ptr = ptr.add(1);
    }
    ptr
}

pub fn encode_varint_u32(value: u32) -> Vec<u8> {
    let mut result = Vec::new();
    let b = 128;
    let mut value = value;

    while value >= b {
        result.push(((value & (b - 1)) | b) as u8);
        value >>= 7;
    }
    result.push(value as u8);

    result
}
pub fn varint_length(value: u32) -> u32 {
    let mut len: u32 = 1;
    let mut value = value;
    while value >= 128 {
        value = value >> 7;
        len += 1;
    }
    len
}

pub fn decode_varint_u32(src: &[u8]) -> Option<(u32, usize)> {
    let mut shift = 0;
    let mut result = 0u32;
    let mut len = 0;
    for &byte in src.iter() {
        len += 1;
        let byte_val = byte as u32;
        result |= (byte_val & 0x7F) << shift;
        if byte_val & 0x80 == 0 {
            return Some((result, len));
        }
        shift += 7;
    }
    None
}

pub fn decode_varint_u64(src: &[u8]) -> Option<(u64, usize)> {
    let mut shift = 0;
    let mut result = 0u64;
    let mut len = 0;
    for &byte in src.iter() {
        len += 1;
        let byte_val = byte as u64;
        result |= (byte_val & 0x7F) << shift;
        if byte_val & 0x80 == 0 {
            return Some((result, len));
        }
        shift += 7;
    }
    None
}

pub fn encode_slice(mut ptr: *mut u8, s: &Vec<u8>) -> *mut u8 {
    unsafe {
        for &x in s {
            *ptr = x;
            ptr = ptr.add(1);
        }
    }
    ptr
}

mod tests {
    use super::*;

    #[test]
    fn test_encode_var() {
        let mut v: Vec<u8> = Vec::new();
        let new_len = 100;
        v.resize(new_len, 0);

        let key = "hello";
        let value = " world";
        let mut ptr = v.as_mut_ptr();
        let data = ptr;
        let ptr = encode_varint_u32_ptr(ptr, key.len() as u32);
        let ptr = encode_varint_u32_ptr(ptr, value.len() as u32);

        let x = decode_varint_u32(&v).unwrap();
        assert_eq!(x.0, key.len() as u32);
        let y = decode_varint_u32(&v[x.1..]).unwrap();
        assert_eq!(y.0, value.len() as u32);

        let ptr = encode_slice(ptr, &key.as_bytes().to_vec());
        let ptr = encode_slice(ptr, &value.as_bytes().to_vec());

        let key_decoded = &v[x.1 + y.1..][..x.0 as usize];
        let val_decoded = &v[x.1 + y.1 + x.0 as usize..][..y.0 as usize];

        assert_eq!(key_decoded, key.as_bytes());
        assert_eq!(val_decoded, value.as_bytes());

    }
}
