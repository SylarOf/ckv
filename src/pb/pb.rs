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
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChangeSet {
    /// a set of changes that are applied atomically
    #[prost(message, repeated, tag = "1")]
    pub changes: ::prost::alloc::vec::Vec<ManifestChange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChange {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(enumeration = "manifest_change::Operation", tag = "2")]
    pub op: i32,
    #[prost(uint32, tag = "3")]
    pub level: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub checksum: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `ManifestChange`.
pub mod manifest_change {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Operation {
        Create = 0,
        Delete = 1,
    }
    impl Operation {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Create => "CREATE",
                Self::Delete => "DELETE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "CREATE" => Some(Self::Create),
                "DELETE" => Some(Self::Delete),
                _ => None,
            }
        }
    }
}
