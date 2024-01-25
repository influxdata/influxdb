//! The encoding mechanism for list streams
//!
//! This is capable of streaming both keys and values, this saves round-trips when hydrating
//! a cache from a remote, and avoids creating a flood of HTTP GET requests

use bytes::Bytes;
use snafu::{ensure, Snafu};

use crate::{CacheKey, CacheValue};

/// Error type for list streams
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Unexpected EOF whilst decoding list stream"))]
    UnexpectedEOF,

    #[snafu(display("List value of {size} bytes too large"))]
    ValueTooLarge { size: usize },
}

/// Result type for list streams
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The size at which to flush [`Bytes`] from [`ListEncoder`]
pub const FLUSH_SIZE: usize = 1024 * 1024; // Flush in 1MB blocks

/// The maximum value size to send in a [`ListEntry`]
///
/// This primarily exists as a self-protection limit to prevent large or corrupted streams
/// from swamping the client, but also mitigates Head-Of-Line blocking resulting from
/// large cache values
pub const MAX_VALUE_SIZE: usize = 1024 * 10;

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
                    buf.extend_from_slice(&entry.header(false).encode());
                    buf.extend_from_slice(d)
                }
                _ => buf.extend_from_slice(&entry.header(true).encode()),
            }
        }
        self.offset = end_offset;
        Some(buf.into())
    }
}

#[allow(non_snake_case)]
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
    pub fn decode(&mut self, mut buf: &[u8]) -> Result<usize> {
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
    pub fn flush(&mut self) -> Result<Option<ListEntry>> {
        match std::mem::take(&mut self.state) {
            DecoderState::Body(header, value) if value.len() == header.size as usize => {
                Ok(Some(ListEntry {
                    variant: header.variant,
                    key: header.key,
                    generation: header.generation,
                    data: ((header.flags & Flags::HEAD) == 0).then(|| value.into()),
                }))
            }
            DecoderState::Header(_, 0) => Ok(None),
            _ => Err(Error::UnexpectedEOF),
        }
    }
}

/// A key value pair encoded as part of a list
///
/// Unlike [`CacheKey`] and [`CacheValue`] this allows:
///
/// * Non-fatal handling of unknown key variants
/// * The option to not include the value data, e.g. if too large
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListEntry {
    variant: u8,
    generation: u64,
    key: u128,
    data: Option<Bytes>,
}

impl ListEntry {
    /// Create a new [`ListEntry`] from the provided key and value
    pub fn new(key: CacheKey, value: CacheValue) -> Self {
        let (variant, key) = match key {
            CacheKey::Namespace(v) => (b'n', v as _),
            CacheKey::Table(v) => (b't', v as _),
            CacheKey::Partition(v) => (b'p', v as _),
        };

        Self {
            key,
            variant,
            generation: value.generation,
            data: Some(value.data),
        }
    }

    /// The key if it matches a known variant of [`CacheKey`]
    ///
    /// Returns `None` otherwise
    pub fn key(&self) -> Option<CacheKey> {
        match self.variant {
            b't' => Some(CacheKey::Table(self.key as _)),
            b'n' => Some(CacheKey::Namespace(self.key as _)),
            b'p' => Some(CacheKey::Partition(self.key as _)),
            _ => None,
        }
    }

    /// The generation of this entry
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the value data if present
    pub fn value(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// Returns the [`ListHeader`] for a given [`ListEntry`]
    fn header(&self, head: bool) -> ListHeader {
        let generation = self.generation;
        let (flags, size) = match (head, &self.data) {
            (false, Some(data)) => (0, data.len() as u32),
            _ => (Flags::HEAD, 0),
        };

        ListHeader {
            size,
            flags,
            variant: self.variant,
            key: self.key,
            generation,
            reserved: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use std::io::BufRead;

    fn decode_entries<R: BufRead>(mut r: R) -> Result<Vec<ListEntry>> {
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
                variant: 0,
                key: u128::MAX,
                generation: u64::MAX,
                data: Some("unknown".into()),
            },
            ListEntry::new(CacheKey::Table(6), CacheValue::new("3".into(), 23)),
            ListEntry {
                variant: b'p',
                key: 45,
                generation: 23,
                data: None,
            },
            ListEntry::new(
                CacheKey::Partition(3),
                CacheValue::new("bananas".into(), 23),
            ),
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

        let mut encoder = ListEncoder::new(entries);
        encoder.flush_size = 1024; // Lower limit for test

        let mut remaining = 1024;
        for block in encoder {
            let expected = remaining.min(7);
            assert_eq!(block.len(), (data.len() + ListHeader::SIZE) * expected);
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

        let mut encoder = ListEncoder::new(entries);
        encoder.max_value_size = 128; // Artificially lower limit for test

        let encoded: Vec<_> = encoder.collect();
        assert_eq!(encoded.len(), 1);

        let decoded = decode_entries(encoded[0].clone().reader()).unwrap();
        assert_eq!(decoded[0].value().unwrap().len(), 128);
        assert_eq!(decoded[1].value(), None); // Should omit value that is too large
        assert_eq!(decoded[2].value().unwrap().len(), 128);

        let mut decoder = ListDecoder::new();
        decoder.max_size = 12;
        let err = decoder.decode(&encoded[0]).unwrap_err().to_string();
        assert_eq!(err, "List value of 128 bytes too large");

        let mut decoder = ListDecoder::new();
        decoder.max_size = 128;

        let consumed = decoder.decode(&encoded[0]).unwrap();
        let r = decoder.flush().unwrap().unwrap();
        assert_eq!(r.value().unwrap().len(), 128);

        // Next record skipped by encoder as too large
        decoder.decode(&encoded[0][consumed..]).unwrap();
        let r = decoder.flush().unwrap().unwrap();
        assert_eq!(r.value(), None);
    }
}
