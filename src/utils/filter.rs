use super::slice::Slice;
pub trait Filter {
    fn with_keys(keys: &[Slice])->Self;
    fn key_may_match(&self, key: &Slice) -> bool;
}
