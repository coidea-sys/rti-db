//! rti-store: columnar storage engine.
//!
//! Write path: `MemTable` (in-memory, slab-pooled) → seal when full →
//! `SegmentWriter` persists it as an immutable columnar segment.
//! Read path: `SegmentReader` skips irrelevant segments via zone maps;
//! timestamp column delta-of-delta + varint, value column XOR compression; streaming decode with zero allocation.

#![forbid(unsafe_code)]

pub mod cold;
pub mod encode;
mod memtable;
mod segment;

pub use cold::{ColdTier, LocalFsColdTier};
#[cfg(feature = "s3")]
pub use cold::{MockS3Server, S3ColdTier, S3Config};
pub use encode::{decode_ts_block, decode_val_block};
pub use memtable::MemTable;
pub use segment::{DecodeIter, SegmentReader, SegmentWriter, ZoneMap, MAGIC};
