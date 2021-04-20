//! Types for reading and writing TSM files produced by InfluxDB >= 2.x

use super::*;
use integer_encoding::VarInt;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::u64;

/// `TSMIndexReader` allows you to read index data within a TSM file.
///
/// # Example
///
/// Iterating over the TSM index.
///
/// ```
/// # use influxdb_tsm::reader::*;
/// # use flate2::read::GzDecoder;
/// # use std::fs::File;
/// # use std::io::BufReader;
/// # use std::io::Cursor;
/// # use std::io::Read;
/// # let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
/// # let mut decoder = GzDecoder::new(file.unwrap());
/// # let mut buf = Vec::new();
/// # decoder.read_to_end(&mut buf).unwrap();
/// # let data_len = buf.len();
/// # let r = Cursor::new(buf);
///
/// let reader = TsmIndexReader::try_new(BufReader::new(r), 4_222_248).unwrap();
///
/// // reader allows you to access each index entry, and each block for each
/// // entry in order.
/// for index_entry in reader {
///     match index_entry {
///         Ok(entry) => {
///             let key = entry.parse_key().unwrap();
///             println!(
///                 "bucket id is {:?}, measurement name is {:?}",
///                 entry.bucket_id(),
///                 key.measurement,
///             )
///         }
///         Err(e) => println!("got an error {:?}", e),
///     }
/// }
/// ```
#[derive(Debug)]
pub struct TsmIndexReader<R>
where
    R: Read + Seek,
{
    r: R,

    curr_offset: u64,
    end_offset: u64,

    curr: Option<IndexEntry>,
    next: Option<IndexEntry>,
}

impl<R> TsmIndexReader<R>
where
    R: Read + Seek,
{
    pub fn try_new(mut r: R, len: usize) -> Result<Self, TsmError> {
        // determine offset to index, which is held in last 8 bytes of file.
        r.seek(SeekFrom::End(-8))?;
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;

        let index_offset = u64::from_be_bytes(buf);
        r.seek(SeekFrom::Start(index_offset))?;

        Ok(Self {
            r,
            curr_offset: index_offset,
            end_offset: len as u64 - 8,
            curr: None,
            next: None,
        })
    }

    /// next_index_entry will return either the next index entry in a TSM file's
    /// index or will return an error. `next_index_entry` updates the offset on
    /// the Index, but it's the caller's responsibility to stop reading entries
    /// when the index has been exhausted.
    fn next_index_entry(&mut self) -> Result<IndexEntry, TsmError> {
        // read length of series key
        let mut buf = [0u8; 2];
        self.r.read_exact(&mut buf)?;
        self.curr_offset += 2;
        let key_len = u16::from_be_bytes(buf);

        // read the series key itself
        let mut key_bytes = vec![0; key_len as usize]; // TODO(edd): re-use this
        self.r.read_exact(key_bytes.as_mut_slice())?;
        self.curr_offset += key_len as u64;

        // read the block type
        self.r.read_exact(&mut buf[..1])?;
        self.curr_offset += 1;
        let b_type = buf[0];

        // read how many blocks there are for this entry.
        self.r.read_exact(&mut buf)?;
        self.curr_offset += 2;
        let count = u16::from_be_bytes(buf);

        let typ = BlockType::try_from(b_type)?;
        Ok(IndexEntry {
            key: key_bytes,
            block_type: typ,
            count,
            curr_block: 1,
            block: self.next_block_entry(typ)?,
        })
    }

    /// next_block_entry will return the next block entry within an index entry.
    /// It is the caller's responsibility to stop reading block entries when
    /// they have all been read for an index entry.
    fn next_block_entry(&mut self, typ: BlockType) -> Result<Block, TsmError> {
        // read min time on block entry
        let mut buf = [0u8; 8];
        self.r.read_exact(&mut buf[..])?;
        self.curr_offset += 8;
        let min_time = i64::from_be_bytes(buf);

        // read max time on block entry
        self.r.read_exact(&mut buf[..])?;
        self.curr_offset += 8;
        let max_time = i64::from_be_bytes(buf);

        // read block data offset
        self.r.read_exact(&mut buf[..])?;
        self.curr_offset += 8;
        let offset = u64::from_be_bytes(buf);

        // read block size
        self.r.read_exact(&mut buf[..4])?;
        self.curr_offset += 4;
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        Ok(Block {
            min_time,
            max_time,
            offset,
            typ,
            size,
            reader_idx: 0,
        })
    }
}

impl<R: Read + Seek> Iterator for TsmIndexReader<R> {
    type Item = Result<IndexEntry, TsmError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_offset == self.end_offset {
            // end of entries
            return None;
        }

        match &self.curr {
            Some(curr) => {
                if curr.curr_block < curr.count {
                    // there are more block entries for this index entry. Read
                    // the next block entry.
                    let mut next = curr.clone();
                    match self.next_block_entry(next.block_type) {
                        Ok(block) => next.block = block,
                        Err(e) => return Some(Err(e)),
                    }
                    next.curr_block += 1;
                    self.next = Some(next);
                } else {
                    // no more block entries. Move onto the next entry.
                    match self.next_index_entry() {
                        Ok(entry) => self.next = Some(entry),
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
            None => match self.next_index_entry() {
                Ok(entry) => self.next = Some(entry),
                Err(e) => return Some(Err(e)),
            },
        }

        self.curr = self.next.clone();
        Some(Ok(self.curr.clone().unwrap()))
    }
}

/// `IndexEntry` provides lazy accessors for components of the entry.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    key: Vec<u8>,

    pub block_type: BlockType,
    pub count: u16,
    pub block: Block,
    curr_block: u16,
}

impl IndexEntry {
    /// Get the organization ID that this entry belongs to.
    pub fn org_id(&self) -> InfluxId {
        Self::extract_id_from_slice(&self.key[..8])
    }

    /// Get the bucket ID that this entry belongs to.
    pub fn bucket_id(&self) -> InfluxId {
        Self::extract_id_from_slice(&self.key[8..16])
    }

    fn extract_id_from_slice(data: &[u8]) -> InfluxId {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[..8]);
        InfluxId::from_be_bytes(buf)
    }

    pub fn parse_key(&self) -> Result<ParsedTsmKey, TsmError> {
        key::parse_tsm_key(&self.key).map_err(|e| TsmError {
            description: e.to_string(),
        })
    }
}

/// A BlockDecoder is capable of decoding a block definition into block data
/// (timestamps and value vectors).

pub trait BlockDecoder {
    fn decode(&mut self, block: &Block) -> Result<BlockData, TsmError>;
}

impl<T> BlockDecoder for &mut T
where
    T: BlockDecoder,
{
    fn decode(&mut self, block: &Block) -> Result<BlockData, TsmError> {
        (&mut **self).decode(block)
    }
}

/// MockBlockDecoder implements the BlockDecoder trait. It uses the `min_time`
/// value in a provided `Block` definition as a key to a map of block data,
/// which should be provided on initialisation.
#[derive(Debug, Clone)]
pub struct MockBlockDecoder {
    blocks: BTreeMap<i64, BlockData>,
}

impl MockBlockDecoder {
    pub fn new(blocks: BTreeMap<i64, BlockData>) -> Self {
        Self { blocks }
    }
}

impl BlockDecoder for MockBlockDecoder {
    fn decode(&mut self, block: &Block) -> std::result::Result<BlockData, TsmError> {
        self.blocks.get(&block.min_time).cloned().ok_or(TsmError {
            description: "block not found".to_string(),
        })
    }
}

/// `BlockData` describes the various types of block data that can be held
/// within a TSM file.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockData {
    Float {
        i: usize,
        ts: Vec<i64>,
        values: Vec<f64>,
    },
    Integer {
        i: usize,
        ts: Vec<i64>,
        values: Vec<i64>,
    },
    Bool {
        i: usize,
        ts: Vec<i64>,
        values: Vec<bool>,
    },
    Str {
        i: usize,
        ts: Vec<i64>,
        values: Vec<Vec<u8>>,
    },
    Unsigned {
        i: usize,
        ts: Vec<i64>,
        values: Vec<u64>,
    },
}

impl BlockData {
    /// Initialise an empty `BlockData` with capacity `other.len()` values.
    fn new_from_data(other: &Self) -> Self {
        match other {
            Self::Float { .. } => Self::Float {
                i: 0,
                ts: Vec::with_capacity(other.len()),
                values: Vec::with_capacity(other.len()),
            },
            Self::Integer { .. } => Self::Integer {
                i: 0,
                ts: Vec::with_capacity(other.len()),
                values: Vec::with_capacity(other.len()),
            },
            Self::Bool { .. } => Self::Bool {
                i: 0,
                ts: Vec::with_capacity(other.len()),
                values: Vec::with_capacity(other.len()),
            },
            Self::Str { .. } => Self::Str {
                i: 0,
                ts: Vec::with_capacity(other.len()),
                values: Vec::with_capacity(other.len()),
            },
            Self::Unsigned { .. } => Self::Unsigned {
                i: 0,
                ts: Vec::with_capacity(other.len()),
                values: Vec::with_capacity(other.len()),
            },
        }
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Float { ts, values, .. } => {
                ts.reserve_exact(additional);
                values.reserve_exact(additional);
            }
            Self::Integer { ts, values, .. } => {
                ts.reserve_exact(additional);
                values.reserve_exact(additional);
            }
            Self::Bool { ts, values, .. } => {
                ts.reserve_exact(additional);
                values.reserve_exact(additional);
            }
            Self::Str { ts, values, .. } => {
                ts.reserve_exact(additional);
                values.reserve_exact(additional);
            }
            Self::Unsigned { ts, values, .. } => {
                ts.reserve_exact(additional);
                values.reserve_exact(additional);
            }
        }
    }

    /// Pushes the provided time-stamp value tuple onto the block data.
    pub fn push(&mut self, pair: ValuePair) {
        match pair {
            ValuePair::F64((t, v)) => {
                if let Self::Float { ts, values, .. } = self {
                    ts.push(t);
                    values.push(v);
                } else {
                    panic!("unsupported variant for BlockData::Float");
                }
            }
            ValuePair::I64((t, v)) => {
                if let Self::Integer { ts, values, .. } = self {
                    ts.push(t);
                    values.push(v);
                } else {
                    panic!("unsupported variant for BlockData::Integer");
                }
            }
            ValuePair::Bool((t, v)) => {
                if let Self::Bool { ts, values, .. } = self {
                    ts.push(t);
                    values.push(v);
                } else {
                    panic!("unsupported variant for BlockData::Bool");
                }
            }
            ValuePair::Str((t, v)) => {
                if let Self::Str { ts, values, .. } = self {
                    ts.push(t);
                    values.push(v); // TODO(edd): figure out
                } else {
                    panic!("unsupported variant for BlockData::Str");
                }
            }
            ValuePair::U64((t, v)) => {
                if let Self::Unsigned { ts, values, .. } = self {
                    ts.push(t);
                    values.push(v);
                } else {
                    panic!("unsupported variant for BlockData::Unsigned");
                }
            }
        }
    }

    pub fn next_pair(&mut self) -> Option<ValuePair> {
        if self.is_empty() {
            return None;
        }

        match self {
            Self::Float { i, ts, values } => {
                let idx = *i;
                *i += 1;
                Some(ValuePair::F64((ts[idx], values[idx])))
            }
            Self::Integer { i, ts, values } => {
                let idx = *i;
                *i += 1;
                Some(ValuePair::I64((ts[idx], values[idx])))
            }
            Self::Bool { i, ts, values } => {
                let idx = *i;
                *i += 1;
                Some(ValuePair::Bool((ts[idx], values[idx])))
            }
            Self::Str { i, ts, values } => {
                let idx = *i;
                *i += 1;
                Some(ValuePair::Str((ts[idx], values[idx].clone()))) // TODO - figure out
            }
            Self::Unsigned { i, ts, values } => {
                let idx = *i;
                *i += 1;
                Some(ValuePair::U64((ts[idx], values[idx])))
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self {
            Self::Float { i, ts, .. } => *i == ts.len(),
            Self::Integer { i, ts, .. } => *i == ts.len(),
            Self::Bool { i, ts, .. } => *i == ts.len(),
            Self::Str { i, ts, .. } => *i == ts.len(),
            Self::Unsigned { i, ts, .. } => *i == ts.len(),
        }
    }

    pub fn len(&self) -> usize {
        match &self {
            Self::Float { ts, .. } => ts.len(),
            Self::Integer { ts, .. } => ts.len(),
            Self::Bool { ts, .. } => ts.len(),
            Self::Str { ts, .. } => ts.len(),
            Self::Unsigned { ts, .. } => ts.len(),
        }
    }

    /// Merges multiple blocks of data together.
    ///
    /// For values within the block that have identical timestamps, `merge`
    /// overwrites previous values. Therefore, in order to have "last write
    /// wins" semantics it is important that the provided vector of blocks
    /// is ordered by the wall-clock time the blocks were created.
    pub fn merge(mut blocks: Vec<Self>) -> Self {
        if blocks.is_empty() {
            panic!("merge called with zero blocks");
        } else if blocks.len() == 1 {
            return blocks.remove(0); // only one block; no merging.
        }

        // The merged output block data to be returned
        let mut block_data = Self::new_from_data(&blocks.first().unwrap());

        // buf will hold the next candidates from each of the sorted input
        // blocks.
        let mut buf = vec![None; blocks.len()];

        // TODO(edd): perf - this simple iterator approach will likely be sped
        // up by batch merging none-overlapping sections of candidate inputs.
        loop {
            match Self::refill_buffer(&mut blocks, &mut buf) {
                Some(min_ts) => {
                    let mut next_pair = None;
                    // deduplicate points that have same timestamp.
                    for pair in &mut buf {
                        if let Some(vp) = pair {
                            if vp.timestamp() == min_ts {
                                // remove the data from the candidate buffer so it
                                // can be refilled next time around
                                next_pair = pair.take();
                            }
                        }
                    }

                    if let Some(vp) = next_pair {
                        block_data.push(vp);
                    } else {
                        // TODO(edd): it feels like we should be able to re-jig
                        // this so the compiler can prove that there is always
                        // a next_pair.
                        panic!("value pair missing from buffer");
                    }
                }
                None => return block_data, // all inputs drained
            }
        }
    }

    fn refill_buffer(blocks: &mut [Self], dst: &mut Vec<Option<ValuePair>>) -> Option<i64> {
        let mut min_ts = None;
        for (block, dst) in blocks.iter_mut().zip(dst) {
            if dst.is_none() {
                *dst = block.next_pair();
            }

            if let Some(pair) = dst {
                match min_ts {
                    Some(min) => {
                        if pair.timestamp() < min {
                            min_ts = Some(pair.timestamp());
                        }
                    }
                    None => min_ts = Some(pair.timestamp()),
                }
            };
        }
        min_ts
    }
}

// ValuePair represents a single timestamp-value pair from a TSM block.
#[derive(Debug, PartialEq, Clone)]
pub enum ValuePair {
    F64((i64, f64)),
    I64((i64, i64)),
    Bool((i64, bool)),
    Str((i64, Vec<u8>)),
    U64((i64, u64)),
}

impl ValuePair {
    // The timestamp associated with the value pair.
    pub fn timestamp(&self) -> i64 {
        match *self {
            Self::F64((ts, _)) => ts,
            Self::I64((ts, _)) => ts,
            Self::Bool((ts, _)) => ts,
            Self::Str((ts, _)) => ts,
            Self::U64((ts, _)) => ts,
        }
    }
}

/// `TSMBlockReader` allows you to read and decode TSM blocks from within a TSM
/// file.
#[derive(Debug)]
pub struct TsmBlockReader<R>
where
    R: Read + Seek,
{
    readers: Vec<R>,
}

impl<R> TsmBlockReader<R>
where
    R: Read + Seek,
{
    pub fn new(r: R) -> Self {
        Self { readers: vec![r] }
    }

    pub fn add_reader(&mut self, r: R) {
        self.readers.push(r);
    }
}

impl<R> BlockDecoder for TsmBlockReader<R>
where
    R: Read + Seek,
{
    /// decode a block whose location is described by the provided
    /// `Block`.
    ///
    /// The components of the returned `BlockData` are guaranteed to have
    /// identical lengths.
    fn decode(&mut self, block: &Block) -> Result<BlockData, TsmError> {
        match self.readers.get_mut(block.reader_idx) {
            Some(r) => {
                r.seek(SeekFrom::Start(block.offset))?;

                let mut data: Vec<u8> = vec![0; block.size as usize];
                r.read_exact(&mut data)?;

                // TODO(edd): skip 32-bit CRC checksum at beginning of block for now
                let mut idx = 4;

                // determine the block type
                let block_type = BlockType::try_from(data[idx])?;
                idx += 1;

                // first decode the timestamp block.
                let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES); // 1000 is the max block size
                let (len, n) = u64::decode_var(&data[idx..]); // size of timestamp block
                idx += n;
                encoders::timestamp::decode(&data[idx..idx + (len as usize)], &mut ts).map_err(
                    |e| TsmError {
                        description: e.to_string(),
                    },
                )?;
                idx += len as usize;

                match block_type {
                    BlockType::Float => {
                        // values will be same length as time-stamps.
                        let mut values = Vec::with_capacity(ts.len());
                        encoders::float::decode_influxdb(&data[idx..], &mut values).map_err(
                            |e| TsmError {
                                description: e.to_string(),
                            },
                        )?;

                        Ok(BlockData::Float { i: 0, ts, values })
                    }
                    BlockType::Integer => {
                        // values will be same length as time-stamps.
                        let mut values = Vec::with_capacity(ts.len());
                        encoders::integer::decode(&data[idx..], &mut values).map_err(|e| {
                            TsmError {
                                description: e.to_string(),
                            }
                        })?;

                        Ok(BlockData::Integer { i: 0, ts, values })
                    }
                    BlockType::Bool => {
                        // values will be same length as time-stamps.
                        let mut values = Vec::with_capacity(ts.len());
                        encoders::boolean::decode(&data[idx..], &mut values).map_err(|e| {
                            TsmError {
                                description: e.to_string(),
                            }
                        })?;

                        Ok(BlockData::Bool { i: 0, ts, values })
                    }
                    BlockType::Str => {
                        // values will be same length as time-stamps.
                        let mut values = Vec::with_capacity(ts.len());
                        encoders::string::decode(&data[idx..], &mut values).map_err(|e| {
                            TsmError {
                                description: e.to_string(),
                            }
                        })?;
                        Ok(BlockData::Str { i: 0, ts, values })
                    }
                    BlockType::Unsigned => {
                        // values will be same length as time-stamps.
                        let mut values = Vec::with_capacity(ts.len());
                        encoders::unsigned::decode(&data[idx..], &mut values).map_err(|e| {
                            TsmError {
                                description: e.to_string(),
                            }
                        })?;
                        Ok(BlockData::Unsigned { i: 0, ts, values })
                    }
                }
            }
            None => Err(TsmError {
                description: format!("cannot decode block {:?} with no associated decoder", block),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::i64;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;

    #[test]
    fn read_tsm_index() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = GzDecoder::new(file.unwrap());
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader = TsmIndexReader::try_new(BufReader::new(Cursor::new(buf)), 4_222_248).unwrap();

        assert_eq!(reader.curr_offset, 3_893_272);
        assert_eq!(reader.count(), 2159)
    }

    #[test]
    fn read_tsm_block() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = GzDecoder::new(file.unwrap());
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader = TsmIndexReader::try_new(BufReader::new(Cursor::new(buf)), 4_222_248).unwrap();

        let mut got_blocks = 0;
        let mut got_min_time = i64::MAX;
        let mut got_max_time = i64::MIN;

        // every block in the fixture file is for the 05c19117091a1000 org and
        // 05c19117091a1001 bucket.
        let org_id = InfluxId::new_str("05c19117091a1000").unwrap();
        let bucket_id = InfluxId::new_str("05c19117091a1001").unwrap();

        for index_entry in reader {
            match index_entry {
                Ok(entry) => {
                    // TODO(edd): this is surely not the right way. I should be
                    // returning mutable references from the iterator.
                    let e = entry.clone();
                    got_blocks += e.count as u64;

                    if entry.block.min_time < got_min_time {
                        got_min_time = e.block.min_time;
                    }

                    if entry.block.max_time > got_max_time {
                        got_max_time = e.block.max_time;
                    }

                    assert_eq!(e.org_id(), org_id);
                    assert_eq!(e.bucket_id(), bucket_id);

                    assert!(
                        e.parse_key().is_ok(),
                        "failed to parse key name for {:}",
                        String::from_utf8_lossy(entry.key.as_slice())
                    );
                }
                Err(e) => panic!("{:?} {:?}", e, got_blocks),
            }
        }

        assert_eq!(got_blocks, 2159); // 2,159 blocks in the file
        assert_eq!(got_min_time, 1_590_585_404_546_128_000); // earliest time is 2020-05-27T13:16:44.546128Z
        assert_eq!(got_max_time, 1_590_597_378_379_824_000); // latest time is
                                                             // 2020-05-27T16:
                                                             // 36:18.379824Z
    }

    #[test]
    fn decode_tsm_blocks() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = GzDecoder::new(file.unwrap());
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();
        let r = Cursor::new(buf);

        let mut block_reader = TsmBlockReader::new(BufReader::new(r));

        let block_defs = vec![
            super::Block {
                min_time: 1590585530000000000,
                max_time: 1590590600000000000,
                offset: 5339,
                size: 153,
                typ: BlockType::Float,
                reader_idx: 0,
            },
            super::Block {
                min_time: 1590585520000000000,
                max_time: 1590590600000000000,
                offset: 190770,
                size: 30,
                typ: BlockType::Integer,
                reader_idx: 0,
            },
        ];

        let mut blocks = vec![];
        for def in block_defs {
            blocks.push(block_reader.decode(&def).unwrap());
        }

        for block in blocks {
            // The first integer block in the value should have 509 values in it.
            match block {
                BlockData::Float { ts, values, .. } => {
                    assert_eq!(ts.len(), 507);
                    assert_eq!(values.len(), 507);
                }
                BlockData::Integer { ts, values, .. } => {
                    assert_eq!(ts.len(), 509);
                    assert_eq!(values.len(), 509);
                }
                other => panic!("should not have decoded {:?}", other),
            }
        }
    }

    // This test scans over the entire tsm contents and
    // ensures no errors are returned from the reader.
    fn walk_index_and_check_for_errors(tsm_gz_path: &str) {
        let file = File::open(tsm_gz_path);
        let mut decoder = GzDecoder::new(file.unwrap());
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();
        let data_len = buf.len();

        let mut index_reader =
            TsmIndexReader::try_new(BufReader::new(Cursor::new(&buf)), data_len).unwrap();
        let mut blocks = Vec::new();

        for res in &mut index_reader {
            let entry = res.unwrap();
            let key = entry.parse_key().unwrap();
            assert!(!key.measurement.is_empty());

            blocks.push(entry.block);
        }

        let mut block_reader = TsmBlockReader::new(Cursor::new(&buf));
        for block in blocks {
            block_reader
                .decode(&block)
                .expect("error decoding block data");
        }
    }

    #[test]
    fn check_tsm_cpu_usage() {
        walk_index_and_check_for_errors("../tests/fixtures/cpu_usage.tsm.gz");
    }

    #[test]
    fn check_tsm_000000000000005_000000002() {
        walk_index_and_check_for_errors("../tests/fixtures/000000000000005-000000002.tsm.gz");
    }

    #[test]
    fn refill_buffer() {
        let mut buf = vec![None; 2];
        let mut blocks = vec![
            BlockData::Float {
                i: 0,
                ts: vec![1, 2, 3],
                values: vec![1.2, 2.3, 4.4],
            },
            BlockData::Float {
                i: 0,
                ts: vec![2],
                values: vec![20.2],
            },
        ];

        let mut min_ts = BlockData::refill_buffer(&mut blocks, &mut buf);
        assert_eq!(min_ts.unwrap(), 1);
        assert_eq!(buf[0].take().unwrap(), ValuePair::F64((1, 1.2)));
        assert_eq!(buf[1].take().unwrap(), ValuePair::F64((2, 20.2)));

        // input buffer drained via take calls above - refill
        min_ts = BlockData::refill_buffer(&mut blocks, &mut buf);
        assert_eq!(min_ts.unwrap(), 2);
        assert_eq!(buf[0].take().unwrap(), ValuePair::F64((2, 2.3)));
        assert_eq!(buf[1].take(), None);
    }

    #[test]
    fn merge_blocks() {
        let res = BlockData::merge(vec![
            BlockData::Integer {
                i: 0,
                ts: vec![1, 2, 3],
                values: vec![10, 20, 30],
            },
            BlockData::Integer {
                i: 0,
                ts: vec![2, 4],
                values: vec![200, 300],
            },
        ]);

        assert_eq!(
            res,
            BlockData::Integer {
                i: 0,
                ts: vec![1, 2, 3, 4],
                values: vec![10, 200, 30, 300],
            },
        );
    }
}
