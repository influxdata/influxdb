use crate::storage::StorageError;

use std::io::BufRead;
use std::io::{Read, Seek, SeekFrom};

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

pub struct Index<T: Read> {
    r: T,
    curr_offset: u64,
    end_offset: u64,

    curr: Option<IndexEntry>,
    next: Option<IndexEntry>,
}

impl<T: Read> Index<T> {
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
}

impl<T: Read> Iterator for Index<T> {
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
    block_type: u8,
    count: u16,

    block: Block,
    curr_block: u16,
}

#[derive(Copy, Clone)]
pub struct Block {
    min_time: i64,
    max_time: i64,
    offset: u64,
    size: u32,
}

#[cfg(test)]
mod test {
    use super::*;
    use libflate::gzip::Decoder;
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Cursor;

    #[test]
    fn read_tsm_index() {
        let file =
            File::open("/Users/edd/rust/delorean/src/storage/000000000000005-000000002.tsm.gz");
        let mut decoder = Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let mut reader = TSMFile::new(BufReader::new(Cursor::new(buf)), 4_222_248);
        let index = reader.index().unwrap();

        assert_eq!(index.curr_offset, 3_893_272);
        assert_eq!(index.count(), 2159)
    }
}
