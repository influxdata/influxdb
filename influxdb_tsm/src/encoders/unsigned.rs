use std::error::Error;

/// Encodes a slice of unsigned 64-bit integers into `dst`.
///
/// Deltas between the integers in the input are first calculated, then the
/// deltas are further compressed if possible, either via bit-packing using
/// simple8b or by run-length encoding the deltas if they're all the same.
pub fn encode(src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    let signed = u64_to_i64_vector(src);
    super::integer::encode(&signed, dst)
}

/// Decodes a slice of bytes into a destination vector of unsigned integers.
pub fn decode(src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error>> {
    if src.is_empty() {
        return Ok(());
    }
    let mut signed_results = vec![];
    super::integer::decode(src, &mut signed_results)?;
    dst.clear();
    dst.reserve_exact(signed_results.len() - dst.capacity());
    for s in signed_results {
        dst.push(s as u64);
    }
    Ok(())
}

// Converts a slice of `u64` values to a `Vec<i64>`.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.iter().map(|&x| x as i64).collect()
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {
    use super::super::integer::Encoding;
    use super::super::simple8b;
    use super::*;

    #[test]
    fn encode_no_values() {
        let src: Vec<u64> = vec![];
        let mut dst = vec![];

        // check for error
        encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_uncompressed() {
        let src: Vec<u64> = vec![1000, 0, simple8b::MAX_VALUE, 213123421];
        let mut dst = vec![];

        let exp = src.clone();
        encode(&src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, Encoding::Uncompressed as u8);
        let mut got = vec![];
        decode(&dst, &mut got).expect("failed to decode");

        // verify got same values back
        assert_eq!(got, exp);
    }

    struct Test {
        name: String,
        input: Vec<u64>,
    }

    #[test]
    fn encode_rle() {
        let tests = vec![
            Test {
                name: String::from("no delta"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("delta increasing"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta decreasing"),
                input: vec![350, 200, 50],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            let exp = test.input;
            encode(&src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(
                &dst[0] >> 4,
                Encoding::Rle as u8,
                "didn't use rle on {src:?}"
            );
            let mut got = vec![];
            decode(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    fn encode_rle_byte_for_byte_with_go() {
        let mut dst = vec![];
        let src = vec![1232342341234u64; 1000];
        encode(&src, &mut dst).expect("failed to encode");

        let expected_encoded = vec![32, 0, 0, 2, 61, 218, 167, 172, 228, 0, 231, 7];
        assert_eq!(dst, expected_encoded);

        assert_eq!(&dst[0] >> 4, Encoding::Rle as u8);
        let mut got = vec![];
        decode(&dst, &mut got).expect("failed to decode");
        assert_eq!(got, src);
    }

    #[test]
    fn encode_simple8b() {
        let tests = vec![Test {
            name: String::from("positive"),
            input: vec![1, 11, 3124, 123543256, 2398567984273478],
        }];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            let exp = test.input;
            encode(&src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[0] >> 4, Encoding::Simple8b as u8);

            let mut got = vec![];
            decode(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    // This tests against a defect found when decoding a TSM block from InfluxDB.
    fn rle_regression() {
        let values = vec![809201799168u64; 509];
        let mut enc = vec![];
        encode(&values, &mut enc).expect("encoding failed");

        // this is a compressed rle integer block representing 509 identical
        // 809201799168 values.
        let enc_influx = [32, 0, 0, 1, 120, 208, 95, 32, 0, 0, 252, 3];

        // ensure that encoder produces same bytes as InfluxDB encoder.
        assert_eq!(enc, enc_influx);

        let mut dec = vec![];
        decode(&enc, &mut dec).expect("failed to decode");

        assert_eq!(dec.len(), values.len());
        assert_eq!(dec, values);
    }
}
