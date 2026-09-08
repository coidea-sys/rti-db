//! rti-store：列式存储引擎。
//!
//! 写入路径：`MemTable`（内存，slab 池化）→ 满则 seal →
//! `SegmentWriter` 落盘为不可变列式 segment。
//! 读取路径：`SegmentReader` 用 zone map 跳过无关段，
//! 时间戳列 delta-of-delta + varint、值列 XOR 压缩，流式解码零分配。

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
