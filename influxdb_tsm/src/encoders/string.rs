use integer_encoding::VarInt;
use std::{convert::TryInto, error::Error};

/// A compressed encoding using Snappy compression. Snappy is the only available
/// string compression format at this time.
const STRING_COMPRESSED_SNAPPY: u8 = 1;
/// The header consists of one byte indicating the compression type.
const HEADER_LEN: usize = 1;
/// Store `i32::MAX` as a `usize` for comparing with lengths in assertions
const MAX_I32: usize = i32::MAX as usize;

/// Encodes a slice of byte slices representing string data into a vector of
/// bytes. Currently uses Snappy compression.
pub fn encode(src: &[&[u8]], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    dst.clear(); // reset buffer
    if src.is_empty() {
        return Ok(());
    }

    // strings shouldn't be longer than 64kb
    let length_of_lengths = src.len() * super::MAX_VAR_INT_32;
    let sum_of_lengths: usize = src
        .iter()
        .map(|s| {
            let len = s.len();
            assert!(len < MAX_I32);
            len
        })
        .sum();
    let source_size = 2 + length_of_lengths + sum_of_lengths;

    // determine the maximum possible length needed for the buffer, which
    // includes the compressed size
    let max_encoded_len = snap::raw::max_compress_len(source_size);
    if max_encoded_len == 0 {
        return Err("source length too large".into());
    }
    let compressed_size = max_encoded_len + HEADER_LEN;
    let total_size = source_size + compressed_size;

    if dst.len() < total_size {
        dst.resize(total_size, 0);
    }

    // write the data to be compressed *after* the space needed for snappy
    // compression. The compressed data is at the start of the allocated buffer,
    // ensuring the entire capacity is returned and available for subsequent use.
    let (compressed_data, data) = dst.split_at_mut(compressed_size);
    let mut n = 0;
    for s in src {
        let len = s.len();
        let len_u64: u64 = len.try_into()?;
        n += len_u64.encode_var(&mut data[n..]);
        data[n..][..len].copy_from_slice(s);
        n += len;
    }
    let data = &data[..n];

    let (header, compressed_data) = compressed_data.split_at_mut(HEADER_LEN);

    header[0] = STRING_COMPRESSED_SNAPPY << 4; // write compression type

    // TODO: snap docs say it is beneficial to reuse an `Encoder` when possible
    let mut encoder = snap::raw::Encoder::new();
    let actual_compressed_size = encoder.compress(data, compressed_data)?;

    dst.truncate(HEADER_LEN + actual_compressed_size);

    Ok(())
}

/// Decodes a slice of bytes representing Snappy-compressed data into a vector
/// of vectors of bytes representing string data, which may or may not be valid
/// UTF-8.
pub fn decode(src: &[u8], dst: &mut Vec<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    if src.is_empty() {
        return Ok(());
    }

    let mut decoder = snap::raw::Decoder::new();
    // First byte stores the encoding type, only have snappy format
    // currently so ignore for now.
    let decoded_bytes = decoder.decompress_vec(&src[HEADER_LEN..])?;

    if dst.capacity() == 0 {
        dst.reserve_exact(64);
    }

    let num_decoded_bytes = decoded_bytes.len();
    let mut i = 0;

    while i < num_decoded_bytes {
        let (length, num_bytes_read) =
            u64::decode_var(&decoded_bytes[i..]).ok_or("invalid encoded string length")?;
        let length: usize = length.try_into()?;

        let lower = i + num_bytes_read;
        let upper = lower + length;

        if upper < lower {
            return Err("length overflow".into());
        }
        if upper > num_decoded_bytes {
            return Err("short buffer".into());
        }

        dst.push(decoded_bytes[lower..upper].to_vec());

        // The length of this string plus the length of the variable byte encoded length
        i += length + num_bytes_read;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_no_values() {
        let src: Vec<&[u8]> = vec![];
        let mut dst = vec![];

        // check for error
        encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.to_vec().len(), 0);
    }

    #[test]
    fn encode_single() {
        let v1_bytes = b"v1";
        let src = vec![&v1_bytes[..]];
        let mut dst = vec![];

        encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst, vec![16, 3, 8, 2, 118, 49]);
    }

    #[test]
    fn encode_multi_compressed() {
        let src_strings: Vec<_> = (0..10).map(|i| format!("value {i}")).collect();
        let src: Vec<_> = src_strings.iter().map(|s| s.as_bytes()).collect();
        let mut dst = vec![];

        encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(
            dst,
            vec![
                16, 80, 28, 7, 118, 97, 108, 117, 101, 32, 48, 13, 8, 0, 49, 13, 8, 0, 50, 13, 8,
                0, 51, 13, 8, 0, 52, 13, 8, 0, 53, 13, 8, 0, 54, 13, 8, 0, 55, 13, 8, 32, 56, 7,
                118, 97, 108, 117, 101, 32, 57
            ]
        );
    }

    #[test]
    fn encode_unicode() {
        let src = vec!["☃".as_bytes()];
        let mut dst = vec![];

        encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst, vec![16, 4, 12, 3, 226, 152, 131]);
    }

    #[test]
    fn encode_invalid_utf8() {
        let src = vec![&[b'\xC0'][..]];
        let mut dst = vec![];

        encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst, vec![16, 2, 4, 1, 192]);
    }

    #[test]
    fn decode_no_values() {
        let src: Vec<u8> = vec![];
        let mut dst = vec![];

        // check for error
        decode(&src, &mut dst).expect("failed to decode src");

        // verify decoded no values.
        assert_eq!(dst.to_vec().len(), 0);
    }

    #[test]
    fn decode_single() {
        let src = vec![16, 3, 8, 2, 118, 49];
        let mut dst = vec![];

        decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        assert_eq!(dst_as_strings, vec!["v1"]);
    }

    #[test]
    fn decode_multi_compressed() {
        let src = vec![
            16, 80, 28, 7, 118, 97, 108, 117, 101, 32, 48, 13, 8, 0, 49, 13, 8, 0, 50, 13, 8, 0,
            51, 13, 8, 0, 52, 13, 8, 0, 53, 13, 8, 0, 54, 13, 8, 0, 55, 13, 8, 32, 56, 7, 118, 97,
            108, 117, 101, 32, 57,
        ];
        let mut dst = vec![];

        decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        let expected: Vec<_> = (0..10).map(|i| format!("value {i}")).collect();
        assert_eq!(dst_as_strings, expected);
    }

    #[test]
    fn decode_unicode() {
        let src = vec![16, 4, 12, 3, 226, 152, 131];
        let mut dst = vec![];

        decode(&src, &mut dst).expect("failed to decode src");

        let dst_as_strings: Vec<_> = dst
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        assert_eq!(dst_as_strings, vec!["☃"]);
    }

    #[test]
    fn decode_invalid_utf8() {
        let src = vec![16, 2, 4, 1, 192];
        let mut dst = vec![];

        decode(&src, &mut dst).expect("failed to decode src");
        assert_eq!(dst, vec![&[b'\xC0'][..]]);
    }
}
