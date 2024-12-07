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

