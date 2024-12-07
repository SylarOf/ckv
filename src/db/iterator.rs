use crate::utils::slice::Slice;
pub trait DBIterator {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek(&mut self, target: &Slice);
    fn next(&mut self);
    fn prev(&mut self);
}
