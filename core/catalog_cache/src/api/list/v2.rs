//! The version 2 list protocol using size prefixed protobuf

use crate::CacheKey;
use crate::api::list::{ListEntry, MAX_VALUE_SIZE, Result, UnexpectedEOFSnafu};
use bytes::{Buf, Bytes};
use futures::{Stream, stream};
use generated_types::google::protobuf::BytesValue;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::prost;
use generated_types::prost::Message;
use reqwest::Response;
use snafu::ensure;
use std::vec::IntoIter;

/// Encodes [`ListEntry`] using the v2 protocol
#[derive(Debug)]
pub struct ListEncoder {
    entries: IntoIter<ListEntry>,
    max_value_size: usize,
}

impl ListEncoder {
    /// Create an encoder for the provided entries
    pub fn new(entries: Vec<ListEntry>) -> Self {
        Self {
            entries: entries.into_iter(),
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
        use proto::list_entry::Variant;

        let entry = self.entries.next()?;
        let data = match entry.data {
            Some(value) if value.len() <= self.max_value_size => Some(BytesValue { value }),
            _ => None,
        };

        let mut message = proto::ListEntry {
            data,
            generation: entry.generation,
            etag: entry.etag.map(|x| x.to_string()).unwrap_or_default(),
            variant: entry.key.map(|key| match key {
                CacheKey::Root => Variant::Root(Default::default()),
                CacheKey::Namespace(x) => Variant::Namespace(x),
                CacheKey::Table(x) => Variant::Table(x),
                CacheKey::Partition(x) => Variant::Partition(x),
            }),
        };
        let encoded_len: u32 = match message.encoded_len().try_into() {
            Ok(x) => x,
            Err(_) => {
                message.data = None;
                message.encoded_len().try_into().unwrap()
            }
        };

        let mut buf = Vec::with_capacity(encoded_len as usize + 4);
        buf.extend_from_slice(&encoded_len.to_le_bytes());
        message.encode_raw(&mut buf);
        Some(buf.into())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

/// Decode [`ListEntry`] from a stream
#[derive(Debug, Default)]
pub struct ListDecoder(DecoderState);

#[derive(Debug)]
enum DecoderState {
    Size([u8; 4], usize),
    Message(Vec<u8>),
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::Size([0; 4], 0)
    }
}

impl ListDecoder {
    /// Decode the next [`ListEntry`] from `buf`
    ///
    /// Call with an empty `buf` to indicate the end of the stream
    pub fn decode<B: Buf>(&mut self, buf: &mut B) -> Result<Option<ListEntry>> {
        while buf.has_remaining() {
            match &mut self.0 {
                DecoderState::Size(c, offset) => {
                    let message_len = if *offset == 0 && buf.remaining() >= 4 {
                        buf.get_u32_le()
                    } else {
                        // Framing is dictated by hyper, and so we need to handle an entry
                        // split across multiple separate `buf`
                        let chunk = buf.chunk();
                        let to_read = (4 - *offset).min(chunk.len());
                        c[*offset..to_read + *offset].copy_from_slice(&chunk[..to_read]);
                        *offset += to_read;
                        buf.advance(to_read);
                        if *offset < 4 {
                            continue;
                        }
                        u32::from_le_bytes(*c)
                    };

                    let cap = message_len as usize;
                    if buf.remaining() >= cap {
                        self.0 = DecoderState::default();
                        return Ok(Some(Self::from_bytes(buf.copy_to_bytes(cap))?));
                    }
                    self.0 = DecoderState::Message(Vec::with_capacity(cap))
                }
                DecoderState::Message(msg) => {
                    let chunk = buf.chunk();
                    let to_read = chunk.len().min(msg.capacity() - msg.len());
                    msg.extend_from_slice(&chunk[..to_read]);
                    buf.advance(to_read);
                    if msg.len() == msg.capacity() {
                        let msg = std::mem::take(msg);
                        self.0 = DecoderState::default();
                        return Ok(Some(Self::from_bytes(msg.into())?));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns true if at a message boundary
    pub fn is_message_boundary(&self) -> bool {
        matches!(self.0, DecoderState::Size(_, 0))
    }

    fn from_bytes(b: Bytes) -> Result<ListEntry, prost::DecodeError> {
        let msg = proto::ListEntry::decode(b)?;

        let key = msg.variant.map(|x| {
            use proto::list_entry::Variant;
            match x {
                Variant::Root(_) => CacheKey::Root,
                Variant::Namespace(x) => CacheKey::Namespace(x),
                Variant::Table(x) => CacheKey::Table(x),
                Variant::Partition(x) => CacheKey::Partition(x),
            }
        });

        Ok(ListEntry {
            key,
            generation: msg.generation,
            data: msg.data.map(|x| x.value),
            etag: (!msg.etag.is_empty()).then(|| msg.etag.into()),
        })
    }
}

struct ListStreamState {
    response: Response,
    current: Bytes,
    decoder: ListDecoder,
}

impl ListStreamState {
    fn new(response: Response) -> Self {
        Self {
            response,
            current: Default::default(),
            decoder: ListDecoder::default(),
        }
    }
}

/// Decode [`ListEntry`] from a v2 list response
pub fn decode_response(response: Response) -> Result<impl Stream<Item = Result<ListEntry>>> {
    let response = response.error_for_status()?;
    let state = ListStreamState::new(response);
    Ok(stream::try_unfold(state, async move |mut state| {
        loop {
            if state.current.is_empty() {
                match state.response.chunk().await? {
                    Some(new) => state.current = new,
                    None => {
                        ensure!(state.decoder.is_message_boundary(), UnexpectedEOFSnafu);
                        return Ok(None);
                    }
                }
            }

            if let Some(x) = state.decoder.decode(&mut state.current)? {
                return Ok(Some((x, state)));
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CacheValue;

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
            ListEntry::new(
                CacheKey::Table(6),
                CacheValue::new("3".into(), 23).with_etag("foo"),
            ),
            ListEntry {
                key: Some(CacheKey::Partition(45)),
                generation: 23,
                data: None,
                etag: Some("Test".into()),
            },
            ListEntry::new(
                CacheKey::Partition(3),
                CacheValue::new("bananas".into(), 23),
            ),
            ListEntry::new(CacheKey::Root, CacheValue::new("the_root".into(), 1337)),
        ];

        let encoded: Vec<_> = ListEncoder::new(expected.clone()).collect();
        assert_eq!(encoded.len(), expected.len());
        let flattened: Vec<u8> = encoded.into_iter().flatten().collect();

        for buf_size in [1, 3, 5, 12] {
            let mut decoder = ListDecoder::default();
            let mut decoded = Vec::with_capacity(expected.len());
            for mut chunk in flattened.chunks(buf_size) {
                while let Some(x) = decoder.decode(&mut chunk).unwrap() {
                    decoded.push(x);
                }
            }
            assert!(decoder.is_message_boundary());

            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn test_size_limit() {
        let entries = vec![
            ListEntry::new(
                CacheKey::Table(1),
                CacheValue::new(Bytes::from_static(&[0; 20]), 23).with_etag("foo"),
            ),
            ListEntry::new(
                CacheKey::Table(2),
                CacheValue::new(Bytes::from_static(&[4; 10]), 23),
            ),
            ListEntry::new(
                CacheKey::Table(3),
                CacheValue::new(Bytes::from_static(&[0; 3]), 23).with_etag("bar"),
            ),
        ];
        let encoder = ListEncoder::new(entries).with_max_value_size(10);
        let data: Vec<_> = encoder.flatten().collect();

        let mut decode = data.as_slice();
        let mut decoder = ListDecoder::default();
        let entry = decoder.decode(&mut decode).unwrap().unwrap();
        assert_eq!(entry.data, None);
        assert_eq!(entry.key, Some(CacheKey::Table(1)));
        assert_eq!(entry.etag, Some("foo".into()));

        let entry = decoder.decode(&mut decode).unwrap().unwrap();
        assert_eq!(entry.data, Some(Bytes::from_static(&[4; 10])));
        assert_eq!(entry.key, Some(CacheKey::Table(2)));
        assert_eq!(entry.etag, None);

        let entry = decoder.decode(&mut decode).unwrap().unwrap();
        assert_eq!(entry.data, Some(Bytes::from_static(&[0; 3])));
        assert_eq!(entry.key, Some(CacheKey::Table(3)));
        assert_eq!(entry.etag, Some("bar".into()));

        assert_eq!(decode.len(), 0);
        assert!(decoder.is_message_boundary());
    }
}
