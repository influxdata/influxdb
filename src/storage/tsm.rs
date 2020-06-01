use crate::encoders::*;
use crate::storage::StorageError;

use integer_encoding::VarInt;
use std::io::{BufRead, Seek, SeekFrom};
use std::u64;

pub struct TSMFile<R>
where
    R: BufRead + Seek,
{
    r: R,
    len: usize,
}

impl<R> TSMFile<R>
where
    R: BufRead + Seek,
{
    pub fn new(r: R, len: usize) -> TSMFile<R> {
        TSMFile { r, len }
    }

    pub fn index(&mut self) -> Result<Index<&mut R>, StorageError> {
        // determine offset to index, which is held in last 8 bytes of file.
        self.r.seek(SeekFrom::End(-8))?;
        let mut buf: [u8; 8] = [0; 8];
        self.r.read_exact(&mut buf)?;
        let index_offset = u64::from_be_bytes(buf);
        self.r.seek(SeekFrom::Start(index_offset))?;

        let index = Index {
            r: self.r.by_ref(),
            curr_offset: index_offset,
            end_offset: self.len as u64 - 8,
            curr: None,
            next: None,
        };
        Ok(index)
    }
}

pub struct Index<R>
where
    R: BufRead + Seek,
{
    r: R,
    curr_offset: u64,
    end_offset: u64,

    curr: Option<IndexEntry>,
    next: Option<IndexEntry>,
}

impl<R: BufRead + Seek> Index<R> {
    /// read_index_entry will yield either the next index entry in a TSM file's index
    /// or will return an error. read_index_entry updates the offset on the Index
    /// but it's the caller's responsibility to stop reading entries when the index
    /// has been exhausted.
    fn read_index_entry(&mut self) -> Result<IndexEntry, StorageError> {
        // read length of series key
        let mut buf: [u8; 2] = [0; 2];
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
        let block_type = buf[0];

        // read how many blocks there are for this entry.
        self.r.read_exact(&mut buf)?;
        self.curr_offset += 2;
        let count = u16::from_be_bytes(buf);

        Ok(IndexEntry {
            key: key_bytes,
            _org_id: None,
            _bucket_id: None,
            // _measurement: None,
            block_type,
            count,
            curr_block: 1,
            block: self.read_block_entry()?,
        })
    }

    /// read_block_entry will yield the next block entry within an index entry.
    /// It is the caller's responsibility to stop reading block entries when they
    /// have all been read for an index entry.
    fn read_block_entry(&mut self) -> Result<Block, StorageError> {
        // read min time on block entry
        let mut buf: [u8; 8] = [0; 8];
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
            size,
        })
    }

    // decode_block decodes the current block pointed to by the provided index
    // entry, returning two vectors containing the timestamps and values.
    // decode_block will seek back to the original position in the index before
    // returning.
    //
    // The vectors are guaranteed to have the same length, with a maximum length
    // of 1000.
    fn decode_block(&mut self, block: &Block) -> Result<BlockData, StorageError> {
        self.r.seek(SeekFrom::Start(block.offset))?;

        let mut data: Vec<u8> = vec![0; block.size as usize];
        self.r.read_exact(&mut data)?;

        // TODO(edd): skip 32-bit CRC checksum at beginning of block for now
        let mut idx = 4;

        // determine the block type
        let block_type = data[idx];
        idx += 1;

        // first decode the timestamp block.
        let mut ts: Vec<i64> = Vec::with_capacity(1000); // 1000 is the max block size
        let (len, n) = u64::decode_var(&data[idx..]); // size of timestamp block
        idx += n;
        timestamp::decode(&data[idx..idx + (len as usize)], &mut ts).map_err(|e| StorageError {
            description: e.to_string(),
        })?;
        idx += len as usize;

        match block_type {
            0 => {
                // values will be same length as time-stamps.
                let mut values: Vec<f64> = Vec::with_capacity(ts.len());
                float::decode_influxdb(&data[idx..], &mut values).map_err(|e| StorageError {
                    description: e.to_string(),
                })?;

                // seek to original position in index before returning to caller.
                self.r.seek(SeekFrom::Start(self.curr_offset))?;

                Ok(BlockData::Float { ts, values })
            }
            1 => {
                // values will be same length as time-stamps.
                let mut values: Vec<i64> = Vec::with_capacity(ts.len());
                integer::decode(&data[idx..], &mut values).map_err(|e| StorageError {
                    description: e.to_string(),
                })?;
                Ok(BlockData::Integer { ts, values })
            }
            2 => Err(StorageError {
                description: String::from("bool block type unsupported"),
            }),
            3 => Err(StorageError {
                description: String::from("string block type unsupported"),
            }),
            4 => Err(StorageError {
                description: String::from("unsigned integer block type unsupported"),
            }),
            _ => Err(StorageError {
                description: String::from(format!("unsupported block type {:?}", block_type)),
            }),
        }
    }
}

impl<R: BufRead + Seek> Iterator for Index<R> {
    type Item = Result<IndexEntry, StorageError>;

    fn next(&mut self) -> Option<Result<IndexEntry, StorageError>> {
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
                    match self.read_block_entry() {
                        Ok(block) => next.block = block,
                        Err(e) => return Some(Err(e)),
                    }
                    next.curr_block += 1;
                    self.next = Some(next);
                } else {
                    // no more block entries. Move onto the next entry.
                    match self.read_index_entry() {
                        Ok(entry) => self.next = Some(entry),
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
            None => match self.read_index_entry() {
                Ok(entry) => self.next = Some(entry),
                Err(e) => return Some(Err(e)),
            },
        }

        self.curr = self.next.clone();
        Some(Ok(self.curr.clone().unwrap()))
    }
}

#[derive(Clone)]
pub struct IndexEntry {
    key: Vec<u8>,
    _org_id: Option<InfluxID>,
    _bucket_id: Option<InfluxID>,
    // _measurement: Option<&'a str>,
    block_type: u8,
    count: u16,

    block: Block,
    curr_block: u16,
}

impl IndexEntry {
    // org_id returns the organisation ID that this entry belongs to.
    fn org_id(&mut self) -> InfluxID {
        match &self._org_id {
            Some(id) => id.clone(),
            None => {
                let mut buf2: [u8; 8] = [0; 8];

                buf2.copy_from_slice(&self.key[..8]);
                let id = InfluxID::from_be_bytes(buf2);
                self._org_id = Some(id.clone());
                id
            }
        }
    }

    // bucket_id returns the organisation ID that this entry belongs to.
    fn bucket_id(&mut self) -> InfluxID {
        match &self._bucket_id {
            Some(id) => id.clone(),
            None => {
                let mut buf2: [u8; 8] = [0; 8];

                buf2.copy_from_slice(&self.key[8..16]);
                let id = InfluxID::from_be_bytes(buf2);
                self._bucket_id = Some(id.clone());
                id
            }
        }
    }
}

/// BlockData describes the various types of block data that can be held within
/// a TSM file.
enum BlockData<'a> {
    Float { ts: Vec<i64>, values: Vec<f64> },
    Integer { ts: Vec<i64>, values: Vec<i64> },
    Bool { ts: Vec<i64>, values: Vec<bool> },
    Str { ts: Vec<i64>, values: Vec<&'a str> },
    Unsigned { ts: Vec<i64>, values: Vec<u64> },
}

#[derive(Copy, Clone)]
pub struct Block {
    min_time: i64,
    max_time: i64,
    offset: u64,
    size: u32,
}

#[derive(Clone, Debug)]
/// InfluxID represents an InfluxDB ID used in InfluxDB 2.x to represent
/// organization and bucket identifiers.
pub struct InfluxID(u64);

#[allow(dead_code)]
impl InfluxID {
    fn new_str(s: &str) -> Result<InfluxID, StorageError> {
        let v = u64::from_str_radix(s, 16).map_err(|e| StorageError {
            description: e.to_string(),
        })?;
        Ok(InfluxID(v))
    }

    fn from_be_bytes(bytes: [u8; 8]) -> InfluxID {
        InfluxID(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for InfluxID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

impl std::cmp::PartialEq for InfluxID {
    fn eq(&self, r: &InfluxID) -> bool {
        self.0 == r.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libflate::gzip;
    use std::fs::File;
    use std::i64;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;

    #[test]
    fn read_tsm_index() {
        let file = File::open("tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let mut reader = TSMFile::new(BufReader::new(Cursor::new(buf)), 4_222_248);
        let index = reader.index().unwrap();

        assert_eq!(index.curr_offset, 3_893_272);
        assert_eq!(index.count(), 2159)
    }

    #[test]
    fn read_tsm_block() {
        let file = File::open("tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let mut reader = TSMFile::new(BufReader::new(Cursor::new(buf)), 4_222_248);
        let index = reader.index().unwrap();

        let mut got_blocks: u64 = 0;
        let mut got_min_time = i64::MAX;
        let mut got_max_time = i64::MIN;

        // every block in the fixture file is for the 05c19117091a1000 org and
        // 05c19117091a1001 bucket.
        let org_id = InfluxID::new_str("05c19117091a1000").unwrap();
        let bucket_id = InfluxID::new_str("05c19117091a1001").unwrap();

        for index_entry in index {
            match index_entry {
                Ok(entry) => {
                    // TODO(edd): this is surely not the right way. I should be
                    // returning mutable references from the iterator.
                    let mut e = entry.clone();
                    got_blocks += e.count as u64;

                    if entry.block.min_time < got_min_time {
                        got_min_time = e.block.min_time;
                    }

                    if entry.block.max_time > got_max_time {
                        got_max_time = e.block.max_time;
                    }

                    assert_eq!(e.org_id(), org_id);
                    assert_eq!(e.bucket_id(), bucket_id);
                }
                Err(e) => panic!("{:?} {:?}", e, got_blocks),
            }
        }

        assert_eq!(got_blocks, 2159); // 2,159 blocks in the file
        assert_eq!(got_min_time, 1590585404546128000); // earliest time is 2020-05-27T13:16:44.546128Z
        assert_eq!(got_max_time, 1590597378379824000); // latest time is 2020-05-27T16:36:18.379824Z
    }

    #[test]
    fn decode_tsm_blocks() {
        let file = File::open("tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let mut reader = TSMFile::new(BufReader::new(Cursor::new(buf)), 4_222_248);
        let mut index = reader.index().unwrap();

        let mut blocks = vec![];
        // Find the float block with offset 5339 in the file.
        let f64_entry = index
            .find(|e| {
                e.as_ref().unwrap().block.offset == 5339 && e.as_ref().unwrap().block_type == 0_u8
            })
            .unwrap()
            .unwrap();
        let f64_block = &index.decode_block(&f64_entry.block).unwrap();
        blocks.push(f64_block);

        // // Find the first integer block index entry in the file.
        // let i64_entry = index
        //     .find(|e| e.as_ref().unwrap().block_type == 1_u8)
        //     .unwrap()
        //     .unwrap();
        // blocks.push(&index.decode_block(&i64_entry.block).unwrap());

        for block in blocks {
            // The first integer block in the value should have 509 values in it.
            match block {
                BlockData::Float { ts, values } => {
                    assert_eq!(ts.len(), 507);
                    assert_eq!(values.len(), 507);
                }
                BlockData::Integer { ts, values } => {
                    assert_eq!(ts.len(), 509);
                    assert_eq!(values.len(), 509);
                }
                BlockData::Bool { ts: _, values: _ } => {
                    panic!("should not have decoded bool block")
                }
                BlockData::Str { ts: _, values: _ } => panic!("should not have decoded str block"),
                BlockData::Unsigned { ts: _, values: _ } => {
                    panic!("should not have decoded unsigned block")
                }
            }
        }
    }

    #[test]
    fn influx_id() {
        let id = InfluxID::new_str("20aa9b0").unwrap();
        assert_eq!(id, InfluxID(34253232));
        assert_eq!(format!("{}", id), "00000000020aa9b0");
    }
}
