//! The version 1 list protocol using a custom byte encoding

use crate::CacheKey;
use crate::api::list::{Error, ListEntry, MAX_VALUE_SIZE, Result, ValueTooLargeSnafu};
use bytes::{Buf, Bytes};
use futures::{Stream, stream};
use reqwest::Response;
use snafu::ensure;

/// The size at which to flush [`Bytes`] from [`ListEncoder`]
pub const FLUSH_SIZE: usize = 1024 * 1024; // Flush in 1MB blocks

/// Encodes [`ListEntry`] as an iterator of [`Bytes`]
///
/// Each [`ListEntry`] is encoded as a `ListHeader`, followed by the value data
#[derive(Debug)]
pub struct ListEncoder {
    /// The current offset into entries
    offset: usize,
    /// The ListEntry to encode
    entries: Vec<ListEntry>,
    /// The flush size, made configurable for testing
    flush_size: usize,
    /// The maximum value size to write
    max_value_size: usize,
}

impl ListEncoder {
    /// Create a new [`ListEncoder`] from the provided [`ListEntry`]
    pub fn new(entries: Vec<ListEntry>) -> Self {
        Self {
            entries,
            offset: 0,
            flush_size: FLUSH_SIZE,
            max_value_size: MAX_VALUE_SIZE,
        }
    }

    /// Override the maximum value size to write
    pub fn with_max_value_size(mut self, size: usize) -> Self {
        self.max_value_size = size;
        self
    }

    /// Override the flush size
    pub fn with_flush_size(mut self, size: usize) -> Self {
        self.flush_size = size;
        self
    }
}

impl Iterator for ListEncoder {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.entries.len() {
            return None;
        }

        let mut cap = 0;
        let mut end_offset = self.offset;
        while end_offset < self.entries.len() && cap < self.flush_size {
            match &self.entries[end_offset].data {
                Some(d) if d.len() <= self.max_value_size => cap += ListHeader::SIZE + d.len(),
                _ => cap += ListHeader::SIZE,
            };
            end_offset += 1;
        }

        let mut buf = Vec::with_capacity(cap);
        for entry in self.entries.iter().take(end_offset).skip(self.offset) {
            match &entry.data {
                Some(d) if d.len() <= self.max_value_size => {
                    buf.extend_from_slice(&ListHeader::new(entry, false).encode());
                    buf.extend_from_slice(d)
                }
                _ => buf.extend_from_slice(&ListHeader::new(entry, true).encode()),
            }
        }
        self.offset = end_offset;
        Some(buf.into())
    }
}

#[expect(non_snake_case)]
mod Flags {
    /// The value is not included in this response
    ///
    /// [`ListEncoder`](super::ListEncoder) only sends inline values for values smaller than a
    /// configured threshold
    pub(crate) const HEAD: u8 = 1;
}

/// The header encoded in a list stream
#[derive(Debug)]
struct ListHeader {
    /// The size of the value
    size: u32,
    /// Reserved for future usage
    reserved: u16,
    /// A bitmask of [`Flags`]
    flags: u8,
    /// The variant of [`CacheKey`]
    variant: u8,
    /// The generation of this value
    generation: u64,
    /// The key contents of [`CacheKey`]
    key: u128,
}

impl ListHeader {
    /// The encoded size of [`ListHeader`]
    const SIZE: usize = 32;

    /// Encodes [`ListHeader`] to an array
    fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0; Self::SIZE];
        out[..4].copy_from_slice(&self.size.to_le_bytes());
        out[4..6].copy_from_slice(&self.reserved.to_le_bytes());
        out[6] = self.flags;
        out[7] = self.variant;
        out[8..16].copy_from_slice(&self.generation.to_le_bytes());
        out[16..32].copy_from_slice(&self.key.to_le_bytes());
        out
    }

    /// Decodes [`ListHeader`] from an array
    fn decode(buf: [u8; Self::SIZE]) -> Self {
        Self {
            size: u32::from_le_bytes(buf[..4].try_into().unwrap()),
            reserved: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            flags: buf[6],
            variant: buf[7],
            generation: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            key: u128::from_le_bytes(buf[16..32].try_into().unwrap()),
        }
    }

    /// Returns the [`ListHeader`] for a given [`ListEntry`]
    fn new(entry: &ListEntry, head: bool) -> Self {
        let generation = entry.generation;
        let (flags, size) = match (head, &entry.data) {
            (false, Some(data)) => (0, data.len() as u32),
            _ => (Flags::HEAD, 0),
        };

        let (variant, key) = match entry.key() {
            Some(CacheKey::Root) => (b'r', 0),
            Some(CacheKey::Namespace(v)) => (b'n', v as _),
            Some(CacheKey::Table(v)) => (b't', v as _),
            Some(CacheKey::Partition(v)) => (b'p', v as _),
            None => (0, 0),
        };

        Self {
            size,
            flags,
            variant,
            key,
            generation,
            reserved: 0,
        }
    }
}

/// The state for [`ListDecoder`]
#[derive(Debug)]
enum DecoderState {
    /// Decoding a header, contains the decoded data and the current offset
    Header([u8; ListHeader::SIZE], usize),
    /// Decoding value data for the given [`ListHeader`]
    Body(ListHeader, Vec<u8>),
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::Header([0; ListHeader::SIZE], 0)
    }
}

/// Decodes [`ListEntry`] from a stream of bytes
#[derive(Debug)]
pub struct ListDecoder {
    state: DecoderState,
    max_size: usize,
}

impl Default for ListDecoder {
    fn default() -> Self {
        Self {
            state: DecoderState::default(),
            max_size: MAX_VALUE_SIZE,
        }
    }
}

impl ListDecoder {
    /// Create a new [`ListDecoder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the maximum value to deserialize
    ///
    /// Values larger than this will result in an error
    /// Defaults to [`MAX_VALUE_SIZE`]
    pub fn with_max_value_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Decode an entry from `buf`, returning the number of bytes consumed
    ///
    /// This is meant to be used in combination with [`Self::flush`]
    pub fn decode(&mut self, mut buf: &[u8]) -> crate::api::list::Result<usize> {
        let initial = buf.len();
        while !buf.is_empty() {
            match &mut self.state {
                DecoderState::Header(header, offset) => {
                    let to_read = buf.len().min(ListHeader::SIZE - *offset);
                    header[*offset..*offset + to_read].copy_from_slice(&buf[..to_read]);
                    *offset += to_read;
                    buf = &buf[to_read..];

                    if *offset == ListHeader::SIZE {
                        let header = ListHeader::decode(*header);
                        let size = header.size as _;
                        ensure!(size <= self.max_size, ValueTooLargeSnafu { size });
                        self.state = DecoderState::Body(header, Vec::with_capacity(size))
                    }
                }
                DecoderState::Body(header, value) => {
                    let to_read = buf.len().min(header.size as usize - value.len());
                    if to_read == 0 {
                        break;
                    }
                    value.extend_from_slice(&buf[..to_read]);
                    buf = &buf[to_read..];
                }
            }
        }
        Ok(initial - buf.len())
    }

    /// Flush the contents of this [`ListDecoder`]
    ///
    /// Returns `Ok(Some(entry))` if a record is fully decoded
    /// Returns `Ok(None)` if no in-progress record
    /// Otherwise returns an error
    pub fn flush(&mut self) -> crate::api::list::Result<Option<ListEntry>> {
        match std::mem::take(&mut self.state) {
            DecoderState::Body(header, value) if value.len() == header.size as usize => {
                let key = match header.variant {
                    b'r' => Some(CacheKey::Root),
                    b't' => Some(CacheKey::Table(header.key as _)),
                    b'n' => Some(CacheKey::Namespace(header.key as _)),
                    b'p' => Some(CacheKey::Partition(header.key as _)),
                    _ => None,
                };

                Ok(Some(ListEntry {
                    key,
                    generation: header.generation,
                    data: ((header.flags & Flags::HEAD) == 0).then(|| value.into()),
                    etag: None,
                }))
            }
            DecoderState::Header(_, 0) => Ok(None),
            _ => Err(Error::UnexpectedEOF),
        }
    }
}

struct ListStreamState {
    response: Response,
    current: Bytes,
    decoder: ListDecoder,
}

impl ListStreamState {
    fn new(response: Response, max_value_size: usize) -> Self {
        Self {
            response,
            current: Default::default(),
            decoder: ListDecoder::new().with_max_value_size(max_value_size),
        }
    }
}

/// Decode [`ListEntry`] from a [`Response`]
pub fn decode_response(
    response: Response,
    max_value_size: usize,
) -> Result<impl Stream<Item = Result<ListEntry>>> {
    let resp = response.error_for_status()?;
    let state = ListStreamState::new(resp, max_value_size);
    Ok(stream::try_unfold(state, async move |mut state| {
        loop {
            if state.current.is_empty() {
                match state.response.chunk().await? {
                    Some(new) => state.current = new,
                    None => break,
                }
            }

            let to_read = state.current.len();
            let read = state.decoder.decode(&state.current)?;
            state.current.advance(read);
            if read != to_read {
                break;
            }
        }
        Ok(state.decoder.flush()?.map(|entry| (entry, state)))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CacheKey, CacheValue};
    use bytes::Buf;
    use std::io::BufRead;

    fn decode_entries<R: BufRead>(mut r: R) -> crate::api::list::Result<Vec<ListEntry>> {
        let mut decoder = ListDecoder::default();
        let iter = std::iter::from_fn(move || {
            loop {
                let buf = r.fill_buf().unwrap();
                if buf.is_empty() {
                    break;
                }
                let to_read = buf.len();
                let read = decoder.decode(buf).unwrap();
                r.consume(read);
                if read != to_read {
                    break;
                }
            }
            decoder.flush().transpose()
        });
        iter.collect()
    }

    #[test]
    fn test_roundtrip() {
        let expected = vec![
            ListEntry::new(CacheKey::Namespace(2), CacheValue::new("test".into(), 32)),
            ListEntry::new(CacheKey::Namespace(6), CacheValue::new("3".into(), 4)),
            ListEntry {
                key: None,
                generation: u64::MAX,
                data: Some("unknown".into()),
                etag: None,
            },
            ListEntry::new(CacheKey::Table(6), CacheValue::new("3".into(), 23)),
            ListEntry {
                key: Some(CacheKey::Partition(45)),
                generation: 23,
                data: None,
                etag: None,
            },
            ListEntry::new(
                CacheKey::Partition(3),
                CacheValue::new("bananas".into(), 23),
            ),
            ListEntry::new(CacheKey::Root, CacheValue::new("the_root".into(), 1337)),
        ];

        let encoded: Vec<_> = ListEncoder::new(expected.clone()).collect();
        assert_eq!(encoded.len(), 1); // Expect entries to be encoded in single flush

        for buf_size in [3, 5, 12] {
            let reader = std::io::BufReader::with_capacity(buf_size, encoded[0].clone().reader());
            let entries = decode_entries(reader).unwrap();
            assert_eq!(entries, expected);

            // Invalid key should not be fatal
            assert_eq!(entries[2].key(), None);
            // Head response should not be fatal
            assert_eq!(entries[4].value(), None);
        }
    }

    #[test]
    fn test_empty() {
        let data: Vec<_> = ListEncoder::new(vec![]).collect();
        assert_eq!(data.len(), 0);

        let entries = decode_entries(std::io::Cursor::new([0_u8; 0])).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_flush_size() {
        let data = Bytes::from(vec![0; 128]);
        let entries = (0..1024)
            .map(|x| ListEntry::new(CacheKey::Namespace(x), CacheValue::new(data.clone(), 0)))
            .collect();

        let encoder = ListEncoder::new(entries).with_flush_size(1024);

        let mut remaining = 1024;
        for block in encoder {
            let expected = remaining.min(7);
            assert_eq!(block.len(), (data.len() + 32) * expected);
            let decoded = decode_entries(block.reader()).unwrap();
            assert_eq!(decoded.len(), expected);
            remaining -= expected;
        }
    }

    #[test]
    fn test_size_limit() {
        let entries = vec![
            ListEntry::new(
                CacheKey::Namespace(0),
                CacheValue::new(vec![0; 128].into(), 0),
            ),
            ListEntry::new(
                CacheKey::Namespace(1),
                CacheValue::new(vec![0; 129].into(), 0),
            ),
            ListEntry::new(
                CacheKey::Namespace(2),
                CacheValue::new(vec![0; 128].into(), 0),
            ),
        ];

        let encoder = ListEncoder::new(entries).with_max_value_size(128);

        let encoded: Vec<_> = encoder.collect();
        assert_eq!(encoded.len(), 1);

        let decoded = decode_entries(encoded[0].clone().reader()).unwrap();
        assert_eq!(decoded[0].value().unwrap().len(), 128);
        assert_eq!(decoded[1].value(), None); // Should omit value that is too large
        assert_eq!(decoded[2].value().unwrap().len(), 128);

        let mut decoder = ListDecoder::new().with_max_value_size(12);
        let err = decoder.decode(&encoded[0]).unwrap_err().to_string();
        assert_eq!(err, "List value of 128 bytes too large");

        let mut decoder = ListDecoder::new().with_max_value_size(128);

        let consumed = decoder.decode(&encoded[0]).unwrap();
        let r = decoder.flush().unwrap().unwrap();
        assert_eq!(r.value().unwrap().len(), 128);

        // Next record skipped by encoder as too large
        decoder.decode(&encoded[0][consumed..]).unwrap();
        let r = decoder.flush().unwrap().unwrap();
        assert_eq!(r.value(), None);
    }
}
