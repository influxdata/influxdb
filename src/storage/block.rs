//! Responsible for storing and serialising blocks of time-series data.
//!
//! The block module provides an API for creating, updating, reading and writing blocks of
//! time-series data, in the form of compressed data blocks.
//!
//! Currently the following block types are supported:
//!
//! - f64 (float blocks);
//! - i64 (signed integer blocks);
//!
//! Other block types are ready to be supported when the appropriate encoders
//! have been implemented.
//!
//! Multiple blocks can be stored in a serialised format within the same file.
//! To facilitate that, blocks have an initial portion that is a _fixed size_,
//! with a variable-sized component following a fixed-size value indicating the
//! size of the variable portion. Therefore, it is possible to read the first part
//! of a block and skip the rest if it is not of interest.
//!
//! ## Block Format
//!
//! The contents of a single Block are as follows:
//!
//! - Checksum (4 bytes): can be used to verify integrity of the rest of the block.
//! - Block ID (4 bytes): the ID of the series associated with the block.
//! - Min timestamp (8 bytes): timestamp of the earliest value in the block.
//! - Max timestamp (8 bytes): timestamp of the latest value in the block.
//! - Remaining Size (4 bytes): indicates how many bytes follow in the rest of the block.
//! - Block Type (1 byte): indicates the type of block data to follow (e.g., for an f64, i64, u64,
//!   string or bool).
//! - Block Summary Size (1 byte): the size in bytes of the block's summary.
//! - Block Data Offset (2 bytes): the offset in the block of the beginning of the block data
//!   section.
//! - Block Data Size (4 bytes): the size in bytes of the block data section.
//! - Block Summary Data (N bytes): the block summary section data.
//! - Block Data (N bytes): the block data section.
//!
//! A Block is serialised as follows:
//!
//!```text
//!╔═════════════════════════════════════════════════════════════════BLOCK═════════════════════════════════════════════════════════════════╗
//!║┌────────┐┌──────┐┌────────┐┌────────┐┌──────┐┌───────┐┌────────────┐┌──────────┐┌─────────┐╔═════════════╗╔══════════════════════════╗║
//!║│        ││      ││        ││        ││      ││       ││            ││          ││         │║             ║║                          ║║
//!║│Checksum││  ID  ││Min Time││Max Time││ Rem  ││ Block ││Summary Size││   Data   ││  Data   │║   SUMMARY   ║║           DATA           ║║
//!║│   4B   ││  4B  ││   8B   ││   8B   ││  4B  ││ Type  ││     1B     ││  Offset  ││  Size   │║     <N>     ║║           <N>            ║║
//!║│        ││      ││        ││        ││      ││  1B   ││            ││    2B    ││   4B    │║             ║║                          ║║
//!║│        ││      ││        ││        ││      ││       ││            ││          ││         │║             ║║                          ║║
//!║└────────┘└──────┘└────────┘└────────┘└──────┘└───────┘└────────────┘└──────────┘└─────────┘╚═════════════╝╚══════════════════════════╝║
//!╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
//!```
//!
//! Notice that the first part of the block is all a fixed size: this means that
//! the remainder of a block (and all the work that goes along with de-serialising summaries and
//! data) can be skipped if the block is not of interest, e.g., due to being outside of a
//! time-range.
//!
//! ### Block Summaries
//!
//! Different block types have different Summaries. For example, String Blocks and
//! Bool Blocks only the track within their Summaries the number values encoded
//! in their block data.
//!
//! Integer, Unsigned and Float Blocks, however, track more information in their
//! Block Summaries, including:
//!
//! - Count (var-int): number of values in block;
//! - Sum (var-int): total sum of values in block;
//! - First (var-int): earliest value in block;
//! - Last (var-int): latest value in block;
//! - Min (var-int): smallest value in block;
//! - Max (var-int): largest value in block;
//!
//! String and Bool Summaries serialise in a very similar way:
//!
//!```text
//!╔═STRING/BOOL BLOCK SUMMARY═╗
//!║ ┌───────────────────────┐ ║
//!║ │                       │ ║
//!║ │         COUNT         │ ║
//!║ │        <vint>         │ ║
//!║ │                       │ ║
//!║ │                       │ ║
//!║ └───────────────────────┘ ║
//!╚═══════════════════════════╝
//!```
//!
//! All other block summaries are serialised in a slightly different way.
//!
//! #### FloatBlock Summary
//!
//! Prior to being encoded using var-int encoding, `f64` values are first converted
//! to an unsigned integer representation.
//!
//!```text
//!╔═══════════════FLOAT BLOCK SUMMARY═══════════════╗
//!║┌──────┐┌──────┐┌──────┐┌──────┐┌──────┐┌──────┐ ║
//!║│      ││      ││      ││      ││      ││      │ ║
//!║│COUNT ││ SUM  ││FIRST ││ LAST ││ MIN  ││ MAX  │ ║
//!║│<vint>││<vint>││<vint>││<vint>││<vint>││<vint>│ ║
//!║│      ││      ││      ││      ││      ││      │ ║
//!║│      ││      ││      ││      ││      ││      │ ║
//!║└──────┘└──────┘└──────┘└──────┘└──────┘└──────┘ ║
//!╚═════════════════════════════════════════════════╝
//!```
//!
//! #### IntegerBlock Summary
//!
//! The signed integer block uses a "Big Int" representation for the sum value, to
//! ensure that large i64 values can be summarised correctly in the block. Therefore,
//! storing the sum of the values in the block involves storing three separate values:
//! a fixed size sign value indicating the sign of the sum, the number of bytes
//! the sum is stored in, and the bytes storing the actual sum value.
//!
//! ```text
//!╔═════════════════════════INTEGER BLOCK SUMMARY═════════════════════════╗
//!║┌──────┐┌────────┐┌────────┐┌────────┐┌──────┐┌──────┐┌──────┐┌──────┐ ║
//!║│      ││        ││        ││        ││      ││      ││      ││      │ ║
//!║│COUNT ││SUM SIGN││ SUM N  ││  SUM   ││FIRST ││ LAST ││ MIN  ││ MAX  │ ║
//!║│<vint>││   1B   ││   2B   ││  <N>   ││<vint>││<vint>││<vint>││<vint>│ ║
//!║│      ││        ││        ││        ││      ││      ││      ││      │ ║
//!║│      ││        ││        ││        ││      ││      ││      ││      │ ║
//!║└──────┘└────────┘└────────┘└────────┘└──────┘└──────┘└──────┘└──────┘ ║
//!╚═══════════════════════════════════════════════════════════════════════╝
//! ```
//!
//! #### UnsignedBlock Summary
//!
//! The unsigned block summary is similar to the signed block summary, but does
//! not require a sign value to be stored.
//!
//! ```text
//!╔═══════════════════UNSIGNED BLOCK SUMMARY════════════════════╗
//!║┌──────┐┌────────┐┌────────┐┌──────┐┌──────┐┌──────┐┌──────┐ ║
//!║│      ││        ││        ││      ││      ││      ││      │ ║
//!║│COUNT ││ SUM N  ││  SUM   ││FIRST ││ LAST ││ MIN  ││ MAX  │ ║
//!║│<vint>││   2B   ││  <N>   ││<vint>││<vint>││<vint>││<vint>│ ║
//!║│      ││        ││        ││      ││      ││      ││      │ ║
//!║│      ││        ││        ││      ││      ││      ││      │ ║
//!║└──────┘└────────┘└────────┘└──────┘└──────┘└──────┘└──────┘ ║
//!╚═════════════════════════════════════════════════════════════╝
//! ```
//!
//! ### Block Data
//!
//! The block data contains the compressed (encoded) blocks of timestamp and value
//! data.
//!
//! Every block type stores the data in the same way, but the contents of the data,
//! e.g., encoding algorithm, is different for each type.
//!
//! The format is as follows:
//!
//! ```text
//!╔═════════════BLOCK DATA══════════════╗
//!║┌───────────┐┌──────────┐┌──────────┐║
//!║│           ││          ││          │║
//!║│Timestamps ││Timestamps││  Values  │║
//!║│   Size    ││   <N>    ││   <N>    │║
//!║│  <vint>   ││          ││          │║
//!║│           ││          ││          │║
//!║└───────────┘└──────────┘└──────────┘║
//!╚═════════════════════════════════════╝
//! ```

use crate::encoders::{float, integer, timestamp};
use crate::storage::StorageError;

use integer_encoding::*;
use num::bigint::{BigInt, BigUint};

use std::convert::TryInto;
use std::io::{Seek, SeekFrom, Write};
use std::{u16, u32};

/// BlockType defines all the possible block types.
pub trait BlockType: Sized + Default + Clone + Copy {
    const BYTE_MARKER: u8;
    type BlockSummary: BlockSummary<Self>;
}

impl BlockType for f64 {
    const BYTE_MARKER: u8 = 0;
    type BlockSummary = FloatBlockSummary;
}

impl BlockType for i64 {
    const BYTE_MARKER: u8 = 1;
    type BlockSummary = IntegerBlockSummary;
}

impl BlockType for bool {
    const BYTE_MARKER: u8 = 2;
    type BlockSummary = BoolBlockSummary;
}

impl<'a> BlockType for &'a str {
    const BYTE_MARKER: u8 = 3;
    type BlockSummary = StringBlockSummary<'a>;
}

impl BlockType for u64 {
    const BYTE_MARKER: u8 = 4;
    type BlockSummary = UnsignedBlockSummary;
}

/// Types implementing `Encoder` are able to encode themselves into compressed
/// blocks of data.
pub trait Encoder {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError>;
}

impl Encoder for Vec<f64> {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError> {
        float::encode(&self, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Encoder for Vec<i64> {
    fn encode(&self, dst: &mut Vec<u8>) -> Result<(), StorageError> {
        integer::encode(&self, dst).map_err(|e| StorageError {
            description: e.to_string(),
        })
    }
}

impl Encoder for Vec<u64> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Encoder for Vec<&str> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

impl Encoder for Vec<bool> {
    fn encode(&self, _: &mut Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError {
            description: String::from("not yet implemented"),
        })
    }
}

/// `Hasher` provides a sub-set of the `core::hash::Hasher` API.
///
/// Specifically, only raw byte streams can be written, ensuring that the caller
/// is responsible for specifying the endianness of any values.
pub trait Hasher {
    fn write(&mut self, bytes: &[u8]);
}

impl Hasher for crc32fast::Hasher {
    fn write(&mut self, bytes: &[u8]) {
        core::hash::Hasher::write(self, bytes);
    }
}

/// `BlockSummary` tracks statistics about the contents of the data in a block.
pub trait BlockSummary<T>: Clone
where
    T: Sized,
{
    /// Initialises a new summary if `values` is not empty.
    fn new(values: &[(i64, T)]) -> Option<Self>;

    /// Adds the provided values to the summary. The caller is responsible for
    /// ensuring that the values are ordered by time.
    fn add(&mut self, values: &[(i64, T)]);

    /// Returns the earliest and latest timestamps in the block.
    fn time_range(&self) -> (i64, i64);

    /// Serialises the summary to the provided `Writer`, and produces a checksum
    /// on the provided `Hasher`.
    ///
    /// `write_to` returns the number of bytes written to `w` or any error encountered.
    fn write_to<W: Write, H: Hasher>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError>;
}

#[derive(Default)]
/// `Block` is a container for a compressed block of timestamps and associated values.
///
/// Blocks comprise a server-assigned ID, a `BlockSummary`, and the `BlockData` itself.
/// Adding data to the `Block` will ensure that the summary and data are updated correctly.
///
/// Currently it is the caller's responsibility to ensure that the contents of
/// any values written in are ordered by time, though the `Block` implementation
/// will ensure that values added in subsequent calls to `push` are sorted with
/// respect to the contents of previous calls.
pub struct Block<T>
where
    T: BlockType,
{
    // checksum is only calculated when the block is serialised.
    #[allow(dead_code)]
    checksum: Option<u32>,
    id: u32,
    summary: Option<T::BlockSummary>,
    data: BlockData<T>,
}

impl<T> Block<T>
where
    T: BlockType + Clone,
    Vec<T>: Encoder,
{
    pub fn new(id: u32) -> Block<T> {
        Block {
            checksum: None,
            id,
            summary: None,
            data: BlockData::default(),
        }
    }

    /// `push` adds all timestamps and values to the block.
    /// Note: currently `push` requires `values` to be sorted by timestamp.
    pub fn push(&mut self, values: &[(i64, T)]) {
        match &mut self.summary {
            None => {
                self.summary = T::BlockSummary::new(values);
            }
            Some(header) => header.add(values),
        }
        self.data.push(values);
    }

    /// `values` returns a sorted copy of values in the block, which are guaranteed
    /// to be sorted by timestamp.
    pub fn values(&mut self) -> &[(i64, T)] {
        self.data.values()
    }

    /// `summary` returns the current summary for this block. The summary is updated
    /// whenever new values are pushed into the block.
    pub fn summary(&self) -> &Option<T::BlockSummary> {
        &self.summary
    }

    /// `write_to` serialises the block into the provided writer `w`.
    pub fn write_to<W>(&mut self, w: &mut W) -> Result<usize, StorageError>
    where
        W: Write + Seek,
    {
        // TODO(edd): what about if w is not at offset 0 when passed in? That
        // means offset below needs to be initialised with the correct offset.
        // There are some experimental APIs to do that here: https://doc.rust-lang.org/std/io/trait.Seek.html#method.stream_position
        // But I'm not sure how to proceed in the meantime...

        let summary = self.summary().as_ref().ok_or_else(|| StorageError {
            description: "empty block".to_string(),
        })?;

        // hasher is used to compute a checksum, which will be written to the
        // front of the Block when it's serialised.
        let mut hasher = crc32fast::Hasher::new();

        let mut offset = 0;

        // 4 byte place-holder for checksum.
        offset += w.write(&[0; 4])?;

        // ID.
        let id_bytes = self.id.to_be_bytes();
        offset += w.write(&id_bytes)?;
        hasher.update(&id_bytes);

        // minimum timestamp in block
        let time_range = summary.time_range();
        let min_time_bytes = time_range.0.to_be_bytes();
        offset += w.write(&min_time_bytes)?;
        hasher.update(&min_time_bytes);

        // maximum timestamp in block
        let max_time_bytes = time_range.1.to_be_bytes();
        offset += w.write(&max_time_bytes)?;
        hasher.update(&max_time_bytes);

        // 4 byte remaining block size place-holder
        let remaining_size_offset = offset;
        offset += w.write(&[0; 4])?;

        // write the block type - do not hash for checksum until later.
        let marker_bytes = [T::BYTE_MARKER];
        offset += w.write(&marker_bytes)?;

        // 1 byte place-holder for summary size
        let summary_size_offset = offset;
        offset += w.write(&[0; 1])?;

        // 2 byte place-holder for block data offset
        let data_offset_offset = offset;
        offset += w.write(&[0; 2])?;

        // 4 byte place-holder for block data size
        let data_size_offset = offset;
        offset += w.write(&[0; 4])?;

        // write the summary - n bytes
        let mut summary_hasher = crc32fast::Hasher::new(); // combined later
        let summary_size = summary.write_to(w, &mut summary_hasher)?;
        offset += summary_size;

        // write the data block - n bytes
        let mut data_block_hasher = crc32fast::Hasher::new(); // combined later
        let data_offset = offset;
        let data_size = self.data.write_to(w, &mut data_block_hasher)?;
        offset += data_size;

        // 1 byte for block type + remainder of bytes written into block.
        let remaining_size: u32 = (1 + offset - summary_size_offset)
            .try_into()
            .expect("remaining_size did not fit in u32");

        // seek back and write in the remaining block size.
        w.seek(SeekFrom::Start(
            remaining_size_offset
                .try_into()
                .expect("remaining_size_offset did not fit in u64"),
        ))?;
        w.write(&remaining_size.to_be_bytes())?;
        hasher.update(&remaining_size.to_be_bytes());

        // hash block type for checksum
        hasher.update(&marker_bytes);

        // seek and write in the summary size.
        w.seek(SeekFrom::Start(
            summary_size_offset
                .try_into()
                .expect("summary_size_offset did not fit in u64"),
        ))?;
        let summary_size: u8 = summary_size
            .try_into()
            .expect("summary_size did not fit in u8");
        w.write(&[summary_size])?;
        hasher.update(&[summary_size]);

        // seek and write the data block offset in the reserved offset
        w.seek(SeekFrom::Start(
            data_offset_offset
                .try_into()
                .expect("data_offset_offset did not fit in u64"),
        ))?;
        // ensure we write size in 2 bytes
        let data_offset: u16 = data_offset
            .try_into()
            .expect("data_offset did not fit in u16");
        w.write(&(data_offset).to_be_bytes())?;
        hasher.update(&(data_offset).to_be_bytes());

        // seek and write the data block size in the reserved offset
        w.seek(SeekFrom::Start(
            data_size_offset
                .try_into()
                .expect("data_size_offset did not fit in u64"),
        ))?;
        let data_size: u32 = data_size.try_into().expect("data_size did not fit in u32");

        w.write(&(data_size).to_be_bytes())?;
        hasher.update(&(data_size).to_be_bytes());

        // combine hasher with summary hasher and data block hasher.
        hasher.combine(&summary_hasher);
        hasher.combine(&data_block_hasher);

        // seek back and write the checksum in.
        w.seek(SeekFrom::Start(0))?;
        let checksum = hasher.finalize();
        w.write(&checksum.to_be_bytes())?;

        Ok(offset)
    }
}

/// `BlockData` represents the underlying compressed time-series data, comprising
/// a timestamp block and a value block.
///
/// `BlockData` ensures that data is sorted on read only, maximising write
/// performance.
struct BlockData<T> {
    values: Vec<(i64, T)>, // TODO(edd): this data layout needs to change.
    sorted: bool,          // indicates if the block data is currently sorted.
}

impl<T> Default for BlockData<T> {
    fn default() -> BlockData<T> {
        BlockData {
            values: Vec::default(),
            sorted: true,
        }
    }
}

impl<T> BlockData<T>
where
    T: Clone,
    Vec<T>: Encoder,
{
    fn push(&mut self, values: &[(i64, T)]) {
        if values.is_empty() {
            return;
        }

        if let Some(last) = self.values.last() {
            if last.0 > values.first().unwrap().0 {
                self.sorted = false;
            }
        }
        self.values.extend_from_slice(values);
    }

    // TODO(edd): currently sort will only sort data by timestamp
    fn sort(&mut self) {
        self.values.sort_by(|a, b| a.0.cmp(&b.0));
        self.sorted = true;
    }

    /// `values` sorts the values in the block if necessary and returns a slice of the timestamps
    /// and values in the block.
    fn values(&mut self) -> &[(i64, T)] {
        if !self.sorted {
            self.sort()
        }
        &self.values
    }

    /// `write_to` serialises the block to the provided `Writer`, compressing the
    /// timestamps and values using the most appropriate encoder for the data.
    fn write_to<W, H>(&mut self, w: &mut W, h: &mut H) -> Result<usize, StorageError>
    where
        W: Write,
        H: Hasher,
    {
        // TODO(edd): PERF - this is super inefficient. Better off storing the time
        // stamps and values in separate vectors on BlockData. Need to implement
        // a sort that works across two vectors based on order of one of the
        // vectors.
        //
        // Currently this is cloning all the stamps and values, which is really
        // not good.
        let (ts, values): (Vec<_>, Vec<_>) = self.values.iter().cloned().unzip();

        let mut total = 0;

        // TODO(edd): pool this buffer
        let mut data_buf: Vec<u8> = vec![];
        timestamp::encode(&ts, &mut data_buf).map_err(|e| StorageError {
            description: e.to_string(),
        })?;

        // length 10 is max needed for encoding varint.
        let mut size_buf = vec![0; 10];
        let n = ts.len().encode_var(&mut size_buf);
        total += w.write(&size_buf[..n])?; // timestamp block size
        h.write(&size_buf[..n]);

        total += w.write(&data_buf)?; // timestamp block
        h.write(&data_buf);

        data_buf.clear();
        values.encode(&mut data_buf)?;
        total += w.write(&data_buf)?; // values block
        h.write(&data_buf);

        Ok(total)
    }
}

/// `FloatBlockSummary` provides a summary of a float block, tracking:
///
/// - count of values in block;
/// - total sum of values in block;
/// - first and last values written to the block; and
/// - smallest and largest values written to the block.

// TODO(edd) need to support big float representation...
#[derive(Clone, Default)]
pub struct FloatBlockSummary {
    count: u32, // max number of values in block ~4.2 Billion
    sum: f64,
    first: (i64, f64),
    last: (i64, f64),
    min: f64,
    max: f64,
}

impl BlockSummary<f64> for FloatBlockSummary {
    fn new(values: &[(i64, f64)]) -> Option<FloatBlockSummary> {
        if values.is_empty() {
            return None;
        }

        let mut header = FloatBlockSummary::default();
        let value = values[0];
        header.count += 1;
        header.sum += value.1;
        header.first = value;
        header.last = value;
        header.min = value.1;
        header.max = value.1;

        if values.len() > 1 {
            header.add(&values[1..]);
        }

        Some(header)
    }

    fn add(&mut self, values: &[(i64, f64)]) {
        for value in values {
            let ts = value.0;
            let v = value.1;

            self.count += 1;
            self.sum += v;
            if self.first.0 > ts {
                self.first = (ts, v);
            }
            if self.last.0 < ts {
                self.last = (ts, v);
            }

            if self.min > v {
                self.min = v;
            }
            if self.max < v {
                self.max = v;
            }
        }
    }

    fn time_range(&self) -> (i64, i64) {
        (self.first.0, self.last.0)
    }

    /// `write_to` serialises the summary to the provided writer and calculates a
    /// checksum of the data written. The number of bytes written is returned.
    fn write_to<W, H>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError>
    where
        W: Write,
        H: Hasher,
    {
        let mut total: usize = 0;
        let mut buf = [0; 10]; // Maximum varint size for 64-bit number.
        let n = self.count.encode_var(&mut buf);
        w.write(&buf[..n])?;
        total += n;
        h.write(&buf[..n]);

        for v in &[self.sum, self.first.1, self.last.1, self.min, self.max] {
            let n = v.to_bits().encode_var(&mut buf);
            total += w.write(&buf[..n])?;
            h.write(&buf[..n]);
        }

        Ok(total)
    }
}

/// `IntegerBlockSummary` provides a summary of a signed integer block, tracking:
///
/// - count of values in block;
/// - total sum of values in block;
/// - first and last values written to the block; and
/// - smallest and largest values written to the block.
///
/// `IntegerBlockSummary` maintains the sum using a big int to ensure multiple large
/// values can be summarised in the block.
#[derive(Clone, Default)]
pub struct IntegerBlockSummary {
    count: u32, // max number of values in block ~4.2 Billion
    sum: BigInt,
    first: (i64, i64),
    last: (i64, i64),
    min: i64,
    max: i64,
}

impl BlockSummary<i64> for IntegerBlockSummary {
    fn new(values: &[(i64, i64)]) -> Option<IntegerBlockSummary> {
        if values.is_empty() {
            return None;
        }

        let mut header = IntegerBlockSummary::default();
        let value = values[0];
        header.count += 1;
        header.sum += value.1;
        header.first = value;
        header.last = value;
        header.min = value.1;
        header.max = value.1;

        if values.len() > 1 {
            header.add(&values[1..]);
        }

        Some(header)
    }

    fn add(&mut self, values: &[(i64, i64)]) {
        for value in values {
            let ts = value.0;
            let v = value.1;

            self.count += 1;
            self.sum += v;
            if self.first.0 > ts {
                self.first = (ts, v);
            }
            if self.last.0 < ts {
                self.last = (ts, v);
            }

            if self.min > v {
                self.min = v;
            }
            if self.max < v {
                self.max = v;
            }
        }
    }

    fn time_range(&self) -> (i64, i64) {
        (self.first.0, self.last.0)
    }

    /// `write_to` serialises the summary to the provided writer and calculates a
    /// checksum. The number of bytes written is returned.
    fn write_to<W, H>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError>
    where
        W: Write,
        H: Hasher,
    {
        let mut total: usize = 0;

        let mut buf = [0; 10]; // Maximum varint size for 64-bit number.
        let n = self.count.encode_var(&mut buf);
        w.write(&buf[..n])?;
        total += n;
        h.write(&buf[..n]);

        // the sum for an integer block is stored as a big int.
        // first write out the sign of the integer.
        let (sign, sum_bytes) = self.sum.to_bytes_be();
        let sign_bytes = [sign as u8];
        total += w.write(&sign_bytes)?;
        h.write(&sign_bytes);

        // next, write out the number of bytes needed to store the big int data.
        //
        // TODO(edd): handle this.. In practice we should not need more than
        // 65,535 bytes to represent a BigInt...
        // ensure length written two bytes.
        let len: u16 = sum_bytes
            .len()
            .try_into()
            .expect("sum_bytes.len() did not fit in u16");
        let len_bytes = len.to_be_bytes();
        total += w.write(&len_bytes)?;
        h.write(&len_bytes);

        // finally, write out the variable number of bytes to represent the big
        // int.
        total += w.write(&sum_bytes)?;
        h.write(&sum_bytes);

        // The rest of the summary values are varint encoded i64s.
        for v in &[self.first.1, self.last.1, self.min, self.max] {
            let n = v.encode_var(&mut buf);
            total += w.write(&buf[..n])?;
            h.write(&buf[..n]);
        }

        Ok(total)
    }
}

/// `BoolBlockSummary` provides a summary of a bool block, tracking the count of
/// values in the block.
#[derive(Clone, Default)]
pub struct BoolBlockSummary {
    count: u32, // max number of values in block ~4.2 Billion

    // N.B, the first and last values are used to track timestamps to calculate
    // the time range of the block, they are not serialised to the block summary.
    first: (i64, bool),
    last: (i64, bool),
}

impl BlockSummary<bool> for BoolBlockSummary {
    fn new(values: &[(i64, bool)]) -> Option<BoolBlockSummary> {
        if values.is_empty() {
            return None;
        }

        let mut header = BoolBlockSummary::default();
        header.count += 1;
        header.first = values[0];
        header.last = values[0];

        if values.len() > 1 {
            header.add(&values[1..]);
        }

        Some(header)
    }

    fn add(&mut self, values: &[(i64, bool)]) {
        for value in values {
            let ts = value.0;
            let v = value.1;

            self.count += 1;
            if self.first.0 > ts {
                self.first = (ts, v);
            }
            if self.last.0 < ts {
                self.last = (ts, v);
            }
        }
    }

    fn time_range(&self) -> (i64, i64) {
        (self.first.0, self.last.0)
    }

    /// `write_to` serialises the summary to the provided writer and calculates a
    /// checksum. The number of bytes written is returned.
    fn write_to<W: Write, H: Hasher>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError> {
        let mut buf = [0; 10]; // Maximum varint size for 64-bit number.
        let n = self.count.encode_var(&mut buf);
        let total = w.write(&buf[..n])?;
        h.write(&buf[..n]);

        Ok(total)
    }
}

/// `StringBlockSummary` provides a summary of a string block, tracking the count of
/// values in the block.
#[derive(Clone, Default)]
pub struct StringBlockSummary<'a> {
    count: u32, // max number of values in block ~4.2 Billion

    // N.B, the first and last values are used to track timestamps to calculate
    // the time range of the block, they are not serialised to the block summary.
    first: (i64, &'a str),
    last: (i64, &'a str),
}

impl<'a> BlockSummary<&'a str> for StringBlockSummary<'a> {
    fn new(values: &[(i64, &'a str)]) -> Option<StringBlockSummary<'a>> {
        if values.is_empty() {
            return None;
        }

        let mut header = StringBlockSummary::default();
        header.count += 1;
        header.first = values[0];
        header.last = values[0];

        if values.len() > 1 {
            header.add(&values[1..]);
        }

        Some(header)
    }

    fn add(&mut self, values: &[(i64, &'a str)]) {
        for value in values {
            let ts = value.0;
            let v = value.1;

            self.count += 1;
            if self.first.0 > ts {
                self.first = (ts, v);
            }
            if self.last.0 < ts {
                self.last = (ts, v);
            }
        }
    }

    fn time_range(&self) -> (i64, i64) {
        (self.first.0, self.last.0)
    }

    /// `write_to` serialises the summary to the provided writer and calculates a
    /// checksum. The number of bytes written is returned.
    fn write_to<W: Write, H: Hasher>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError> {
        let mut buf = [0; 10]; // Maximum varint size for 64-bit number.
        let n = self.count.encode_var(&mut buf);
        let total = w.write(&buf[..n])?;
        h.write(&buf[..n]);

        Ok(total)
    }
}

/// `UnsignedBlockSummary` provides a summary of an unsigned integer block, tracking:
///
/// - count of values in block;
/// - total sum of values in block;
/// - first and last values written to the block; and
/// - smallest and largest values written to the block.
///
/// `UnsignedBlockSummary` maintains the sum using a big uint to ensure multiple large
/// values can be summarised in the block.
#[derive(Clone, Default)]
pub struct UnsignedBlockSummary {
    count: u32, // max number of values in block ~4.2 Billion
    sum: BigUint,
    first: (i64, u64),
    last: (i64, u64),
    min: u64,
    max: u64,
}

impl BlockSummary<u64> for UnsignedBlockSummary {
    fn new(values: &[(i64, u64)]) -> Option<UnsignedBlockSummary> {
        if values.is_empty() {
            return None;
        }

        let mut header = UnsignedBlockSummary::default();
        let value = values[0];
        header.count += 1;
        header.sum += value.1;
        header.first = value;
        header.last = value;
        header.min = value.1;
        header.max = value.1;

        if values.len() > 1 {
            header.add(&values[1..]);
        }

        Some(header)
    }

    fn add(&mut self, values: &[(i64, u64)]) {
        for value in values {
            let ts = value.0;
            let v = value.1;

            self.count += 1;
            self.sum += v;
            if self.first.0 > ts {
                self.first = (ts, v);
            }
            if self.last.0 < ts {
                self.last = (ts, v);
            }

            if self.min > v {
                self.min = v;
            }
            if self.max < v {
                self.max = v;
            }
        }
    }

    fn time_range(&self) -> (i64, i64) {
        (self.first.0, self.last.0)
    }

    /// `write_to` serialises the summary to the provided writer and calculates a
    /// checksum. The number of bytes written is returned.
    fn write_to<W, H>(&self, w: &mut W, h: &mut H) -> Result<usize, StorageError>
    where
        W: Write,
        H: Hasher,
    {
        let mut total: usize = 0;

        let mut buf = [0; 10]; // Maximum varint size for 64-bit number.
        let n = self.count.encode_var(&mut buf);
        w.write(&buf[..n])?;
        total += n;
        h.write(&buf[..n]);

        // first, write the number of bytes needed to store the big uint data.
        //
        // TODO(edd): handle this.. In practice we should not need more than
        // 65,535 bytes to represent a BigUint...
        let sum_bytes = self.sum.to_bytes_be();
        // ensure length can be written two bytes.
        let sum_bytes_len: u16 = sum_bytes
            .len()
            .try_into()
            .expect("sum_bytes.len() did not fit in u16");
        let sum_bytes_len_bytes = sum_bytes_len.to_be_bytes();
        total += w.write(&sum_bytes_len_bytes)?;
        h.write(&sum_bytes_len_bytes);

        // finally, write out the variable number of bytes to represent the big
        // int.
        total += w.write(&sum_bytes)?;
        h.write(&sum_bytes);

        // The rest of the summary values are varint encoded i64s.
        for v in &[self.first.1, self.last.1, self.min, self.max] {
            let n = v.encode_var(&mut buf);
            total += w.write(&buf[..n])?;
            h.write(&buf[..n]);
        }

        Ok(total)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::approximately_equal;
    use std::io::Cursor;

    #[test]
    fn f64_block_header_add() {
        let ts = 100;
        let v = 22.32;
        let mut header = FloatBlockSummary::new(&[(ts, v)]).unwrap();
        assert_eq!(header.count, 1);
        assert!(approximately_equal(header.sum, v));
        assert_eq!(header.first, (ts, v));
        assert_eq!(header.last, (ts, v));
        assert!(approximately_equal(header.min, v));
        assert!(approximately_equal(header.max, v));

        header.add(&[(88, 2.2)]);
        assert_eq!(header.count, 2);
        assert!(approximately_equal(header.sum, 24.52));
        assert_eq!(header.first, (88, 2.2));
        assert_eq!(header.last, (100, 22.32));
        assert!(approximately_equal(header.min, 2.2));
        assert!(approximately_equal(header.max, 22.32));

        header.add(&[(191, -123.0)]);
        assert_eq!(header.count, 3);
        assert!(approximately_equal(header.sum, -98.48));
        assert_eq!(header.first, (88, 2.2));
        assert_eq!(header.last, (191, -123.0));
        assert!(approximately_equal(header.min, -123.0));
        assert!(approximately_equal(header.max, 22.32));
    }

    #[test]
    fn f64_block_header_write_to() {
        let header = FloatBlockSummary::new(&[(191, -123.0), (200, 22.0), (300, 0.0)]).unwrap();
        let mut buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        let size = header.write_to(&mut buf, &mut h).unwrap();

        let got = buf.get_ref();
        let exp = vec![
            3, // varint count of 3,
            128, 128, 128, 128, 128, 128, 208, 172, 192, 1, // varint sum of -101
            128, 128, 128, 128, 128, 128, 176, 175, 192, 1, // varint first value
            0, // varint last value
            128, 128, 128, 128, 128, 128, 176, 175, 192, 1, // varint min value -123
            128, 128, 128, 128, 128, 128, 128, 155, 64, // varint max value 22.0
        ];

        assert_eq!(got, &exp);
        assert_eq!(size, 41);
    }

    #[test]
    fn i64_block_header_add() {
        let ts = 100;
        let v = 22;
        let mut header = IntegerBlockSummary::new(&[(ts, v)]).unwrap();
        assert_eq!(header.count, 1);
        assert_eq!(header.sum, BigInt::from(v));
        assert_eq!(header.first, (ts, v));
        assert_eq!(header.last, (ts, v));
        assert_eq!(header.min, v);
        assert_eq!(header.max, v);

        header.add(&[(88, 2)]);
        assert_eq!(header.count, 2);
        assert_eq!(header.sum, BigInt::from(24));
        assert_eq!(header.first, (88, 2));
        assert_eq!(header.last, (100, 22));
        assert_eq!(header.min, 2);
        assert_eq!(header.max, 22);

        header.add(&[(191, -123)]);
        assert_eq!(header.count, 3);
        assert_eq!(header.sum, BigInt::from(-99));
        assert_eq!(header.first, (88, 2));
        assert_eq!(header.last, (191, -123));
        assert_eq!(header.min, -123);
        assert_eq!(header.max, 22);
    }

    #[test]
    fn i64_block_header_write_to() {
        let header = IntegerBlockSummary::new(&[(191, -123), (200, 22), (300, 0)]).unwrap();
        let mut buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        let size = header.write_to(&mut buf, &mut h).unwrap();

        let got = buf.get_ref();
        let exp = vec![
            3, // varint count of 3,
            0, // num_bigint::Sign::Minus (negative sign on sum)
            0, 1,   // bytes needed to represent sum
            101, // bytes representing sum (sum is -101)
            245, 1, // varint encoding first value (-123)
            0, // last value written (0)
            245, 1,  // varint encoding min value (-123)
            44, // varint max value 22
        ];

        assert_eq!(got, &exp);
        assert_eq!(size, 11);
    }

    #[test]
    fn str_block_header_add() {
        let ts = 100;
        let v = "test";
        let mut header = StringBlockSummary::new(&[(ts, v)]).unwrap();
        assert_eq!(header.count, 1);
        assert_eq!(header.first, (ts, v));
        assert_eq!(header.last, (ts, v));

        let v2 = "foo";
        header.add(&[(88, v2)]);
        assert_eq!(header.count, 2);
        assert_eq!(header.first, (88, v2));
        assert_eq!(header.last, (100, v));

        let v3 = "abc";
        header.add(&[(191, v3)]);
        assert_eq!(header.count, 3);
        assert_eq!(header.first, (88, v2));
        assert_eq!(header.last, (191, v3));
    }

    #[test]
    fn str_block_header_write_to() {
        let header = StringBlockSummary::new(&[(191, "hello"), (200, "world")]).unwrap();
        let mut buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        let size = header.write_to(&mut buf, &mut h).unwrap();

        let got = buf.get_ref();
        let exp = vec![
            2, // varint count of 3
        ];

        assert_eq!(got, &exp);
        assert_eq!(size, 1);
    }

    #[test]
    fn bool_block_header_add() {
        let ts = 100;
        let v = true;
        let mut header = BoolBlockSummary::new(&[(ts, v)]).unwrap();
        assert_eq!(header.count, 1);
        assert_eq!(header.first, (ts, v));
        assert_eq!(header.last, (ts, v));

        header.add(&[(88, true)]);
        assert_eq!(header.count, 2);
        assert_eq!(header.first, (88, true));
        assert_eq!(header.last, (100, true));

        header.add(&[(191, false)]);
        assert_eq!(header.count, 3);
        assert_eq!(header.first, (88, true));
        assert_eq!(header.last, (191, false));
    }

    #[test]
    fn bool_block_header_write_to() {
        let header =
            BoolBlockSummary::new(&[(191, true), (200, true), (300, false), (400, false)]).unwrap();
        let mut buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        let size = header.write_to(&mut buf, &mut h).unwrap();

        let got = buf.get_ref();
        let exp = vec![
            4, // varint count of 3
        ];

        assert_eq!(got, &exp);
        assert_eq!(size, 1);
    }

    #[test]
    fn u64_block_header_add() {
        let ts = 100;
        let v = 22;
        let mut header = UnsignedBlockSummary::new(&[(ts, v)]).unwrap();
        assert_eq!(header.count, 1);
        assert_eq!(header.sum, BigUint::from(v));
        assert_eq!(header.first, (ts, v));
        assert_eq!(header.last, (ts, v));
        assert_eq!(header.min, v);
        assert_eq!(header.max, v);

        header.add(&[(88, 2)]);
        assert_eq!(header.count, 2);
        assert_eq!(header.sum, BigUint::from(24_u64));
        assert_eq!(header.first, (88, 2));
        assert_eq!(header.last, (100, 22));
        assert_eq!(header.min, 2);
        assert_eq!(header.max, 22);

        header.add(&[(191, 0)]);
        assert_eq!(header.count, 3);
        assert_eq!(header.sum, BigUint::from(24_u64));
        assert_eq!(header.first, (88, 2));
        assert_eq!(header.last, (191, 0));
        assert_eq!(header.min, 0);
        assert_eq!(header.max, 22);
    }

    #[test]
    fn u64_block_header_write_to() {
        let header =
            UnsignedBlockSummary::new(&[(191, 123), (200, 22), (300, 30), (400, 27)]).unwrap();
        let mut buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        let size = header.write_to(&mut buf, &mut h).unwrap();

        let got = buf.get_ref();
        let exp = vec![
            4, // varint count of 3,
            0, 1,   // bytes needed to represent sum
            202, // bytes representing sum (sum is -101)
            123, // varint encoding first value (123)
            27,  // varint last value written (27)
            22,  // varint encoding min value (22)
            123, // varint max value 123
        ];

        assert_eq!(got, &exp);
        assert_eq!(size, 8);
    }

    #[test]
    fn block_push_values() {
        let mut block: Block<f64> = Block::new(22);
        block.push(&[]); // Pushing nothing is okay.
        assert!(block.values().is_empty());
        assert!(block.summary().is_none());

        block.push(&[(100, 33.221)]);
        block.push(&[(101, 1.232)]);
        block.push(&[(88, 1000.0)]);

        assert_eq!(
            vec![(88, 1000.0), (100, 33.221), (101, 1.232)],
            block.values(),
        );

        block.push(&[(1, 22.22), (2, 19.23), (99, -1234.22)]);

        assert_eq!(
            vec![
                (1, 22.22),
                (2, 19.23),
                (88, 1000.0),
                (99, -1234.22),
                (100, 33.221),
                (101, 1.232)
            ],
            block.values(),
        );

        // Check header is updated.
        let header = block.summary().as_ref().unwrap();
        assert_eq!(header.count, 6);
    }

    #[test]
    fn block_write() {
        let mut block = Block::new(22);
        block.push(&[(1, 2000.1), (2, 200.2), (99, 22.2)]);

        let mut buf = Cursor::new(vec![]);
        let n = block.write_to(&mut buf).unwrap();

        let mut exp = vec![
            130, 140, 69, 138, // checksum
            0, 0, 0, 22, // id
            0, 0, 0, 0, 0, 0, 0, 1, // min timestamp
            0, 0, 0, 0, 0, 0, 0, 99, // max timestamp
            0, 0, 0, 103, // remaining size in block
            0,   // block type
            46,  // summary size
            0, 82, // data offset
            0, 0, 0, 49, // data size
        ];

        // add the summary into expected value
        let mut summary_buf = Cursor::new(vec![]);
        let mut h = crc32fast::Hasher::new();
        block
            .summary
            .unwrap()
            .write_to(&mut summary_buf, &mut h)
            .unwrap();
        exp.extend(summary_buf.get_ref());

        // add the block data into expected value
        let mut data_buf = Cursor::new(vec![]);
        block.data.write_to(&mut data_buf, &mut h).unwrap();
        exp.extend(data_buf.get_ref());

        assert_eq!(buf.get_ref(), &exp);
        assert_eq!(n, buf.get_ref().len());
    }
}
