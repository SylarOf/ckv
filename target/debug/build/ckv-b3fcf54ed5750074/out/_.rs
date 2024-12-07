// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIndex {
    #[prost(message, repeated, tag = "1")]
    pub offsets: ::prost::alloc::vec::Vec<BlockOffset>,
    #[prost(bytes = "vec", tag = "2")]
    pub bloom_filter: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub key_count: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockOffset {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
    #[prost(uint32, tag = "3")]
    pub len: u32,
}
