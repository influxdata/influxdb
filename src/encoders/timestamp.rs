use super::simple8b;
use integer_encoding::*;
use std::error::Error;

// Encoding describes the type of encoding used by an encoded timestamp block.
#[allow(dead_code)]
enum Encoding {
    Uncompressed = 0,
    Simple8b = 1,
    Rle = 2,
}

/// encode_all encodes a vector of signed integers into a slice of bytes.
///
/// To maximise compression, the provided vector should be sorted in ascending
/// order. First deltas between the integers are determined, then further encoding
/// is potentially carried out. If all the deltas are the same the block can be
/// encoded using RLE. If not, as long as the deltas are not bigger than simple8b::MAX_VALUE
/// they can be encoded using simple8b.
#[allow(dead_code)]
pub fn encode_all<'a>(src: &mut Vec<i64>, dst: &'a mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    dst.truncate(0); // reset buffer.
    if src.len() == 0 {
        return Ok(());
    }

    let mut max: u64 = 0;
    let mut deltas = i64_to_u64_vector(src);
    if deltas.len() > 1 {
        for i in (1..deltas.len()).rev() {
            deltas[i] = deltas[i].wrapping_sub(deltas[i - 1]);
            if deltas[i] > max {
                max = deltas[i];
            }
        }
        let mut use_rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                use_rle = false;
                break;
            }
        }

        // Encode with RLE if possible.
        if use_rle {
            encode_rle(deltas[0], deltas[1], deltas.len() as u64, dst);
            // 4 high bits of first byte used for the encoding type
            dst[0] |= (Encoding::Rle as u8) << 4;
            return Ok(());
        }
    }

    // write block uncompressed
    if max > simple8b::MAX_VALUE {
        let cap = 1 + (deltas.len() * 8); // 8 bytes per value plus header byte
        if dst.capacity() < cap {
            dst.reserve_exact(cap - dst.capacity());
        }

        dst.push((Encoding::Uncompressed as u8) << 4);
        for delta in deltas.iter() {
            dst.extend_from_slice(&delta.to_be_bytes());
        }
        return Ok(());
    }

    // Compress with simple8b
    // First find the divisor for the deltas.
    let mut div: u64 = 1_000_000_000_000;
    for delta in deltas.iter().skip(1) {
        if div <= 1 {
            break;
        }
        while div > 1 && delta % div != 0 {
            div /= 10;
        }
    }

    if div > 1 {
        // apply only if expense of division is warranted
        for delta in deltas.iter_mut().skip(1) {
            *delta /= div
        }
    }

    // first 4 high bits used for encoding type
    dst.push((Encoding::Simple8b as u8) << 4);
    dst[0] |= ((div as f64).log10()) as u8; // 4 low bits used for log10 divisor
    dst.extend_from_slice(&deltas[0].to_be_bytes()); // encode first value
    simple8b::encode_all(&deltas[1..], dst)
}

// i64_to_u64_vector converts a Vec<i64> to Vec<u64>.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
#[allow(dead_code)]
fn i64_to_u64_vector(src: &[i64]) -> Vec<u64> {
    src.into_iter().map(|x| *x as u64).collect::<Vec<u64>>()
}

// u64_to_i64_vector converts a Vec<u64> to Vec<i64>.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
#[allow(dead_code)]
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.into_iter().map(|x| *x as i64).collect::<Vec<i64>>()
}

// encode_rle encodes the value v, delta and count into dst.
//
// v should be the first element of a sequence, delta the difference that each
// value in the sequence differs by, and count the total number of values in the
// sequence.
#[allow(dead_code)]
fn encode_rle(v: u64, delta: u64, count: u64, dst: &mut Vec<u8>) {
    let max_var_int_size = 10; // max number of bytes needed to store var int

    // Keep a byte back for the scaler.
    dst.push(0);
    let mut n = 1;
    // write the first value in as a byte array.
    dst.extend_from_slice(&v.to_be_bytes());
    n += 8;

    // check delta's divisor
    let mut div: u64 = 1_000_000_000_000;
    while div > 1 && delta % div != 0 {
        div /= 10;
    }

    if dst.len() <= n + max_var_int_size {
        dst.resize(n + max_var_int_size, 0);
    }

    // 4 low bits are the log10 divisor.
    if div > 1 {
        // calculate and store the number of trailing 0s in the divisor.
        // e.g., 100_000 would be stored as 5.
        let scaler = ((div as f64).log10()) as u8;
        assert!(scaler <= 15);

        dst[0] |= scaler; // Set the scaler on low 4 bits of first byte.
        n += (delta / div).encode_var(&mut dst[n..]);
    } else {
        n += delta.encode_var(&mut dst[n..]);
    }

    if dst.len() - n <= max_var_int_size {
        dst.resize(n + max_var_int_size, 0);
    }
    // finally, encode the number of times the delta is repeated.
    n += count.encode_var(&mut dst[n..]);
    dst.truncate(n);
}

/// decode_all decodes a slice of bytes encoded using encode_all back into a
/// vector of signed integers.
#[allow(dead_code)]
pub fn decode_all<'a>(src: &[u8], dst: &'a mut Vec<i64>) -> Result<(), Box<dyn Error>> {
    if src.len() == 0 {
        return Ok(());
    }
    let encoding = &src[0] >> 4;
    match encoding {
        encoding if encoding == Encoding::Uncompressed as u8 => {
            return decode_uncompressed(&src[1..], dst); // first byte not used
        }
        encoding if encoding == Encoding::Rle as u8 => return decode_rle(&src, dst),
        encoding if encoding == Encoding::Simple8b as u8 => return decode_simple8b(&src, dst),
        _ => return Err(From::from("invalid block encoding")),
    }
}

// decode_uncompressed writes the binary encoded values in src into dst.
#[allow(dead_code)]
fn decode_uncompressed(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error>> {
    if src.len() == 0 || src.len() & 0x7 != 0 {
        return Err(From::from("invalid uncompressed block length"));
    }

    let count = src.len() / 8;
    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }
    let mut i = 0;
    let mut prev = 0;
    let mut buf: [u8; 8] = [0; 8];
    while i < src.len() {
        buf.copy_from_slice(&src[i..i + 8]);
        prev += i64::from_be_bytes(buf);
        dst.push(prev); // N.B - signed integer...
        i += 8;
    }
    Ok(())
}

// decode_rle decodes an RLE encoded slice containing only unsigned into the
// destination vector.
#[allow(dead_code)]
fn decode_rle(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error>> {
    if src.len() < 9 {
        return Err(From::from("not enough data to decode using RLE"));
    }

    // calculate the scaler from the lower 4 bits of the first byte.
    let scaler = 10_u64.pow((src[0] & 0b00001111) as u32);
    let mut i = 1;

    // TODO(edd): this should be possible to do in-place without copy.
    let mut a: [u8; 8] = [0; 8];
    a.copy_from_slice(&src[i..i + 8]);
    i += 8;
    let (mut delta, n) = u64::decode_var(&src[i..]);
    if n <= 0 {
        return Err(From::from("unable to decode delta"));
    }
    i += n;
    delta *= scaler;

    let (count, n) = usize::decode_var(&src[i..]);
    if n <= 0 {
        return Err(From::from("unable to decode count"));
    }

    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }

    let mut first = i64::from_be_bytes(a);
    for _ in 0..count {
        dst.push(first);
        first = first.wrapping_add(delta as i64);
    }
    Ok(())
}

#[allow(dead_code)]
fn decode_simple8b(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<dyn Error>> {
    if src.len() < 9 {
        return Err(From::from("not enough data to decode packed timestamp"));
    }

    let scaler = 10_u64.pow((src[0] & 0b00001111) as u32);

    // TODO(edd): pre-allocate res by counting bytes in encoded slice?
    let mut res = vec![];
    let mut buf: [u8; 8] = [0; 8];
    buf.copy_from_slice(&src[1..9]);
    dst.push(i64::from_be_bytes(buf));

    simple8b::decode_all(&src[9..], &mut res);
    let mut next = dst[dst.len() - 1];
    if scaler > 1 {
        // TODO(edd): fix this. It's copying, which is slowwwwwwwww.
        for v in res.iter() {
            next += (v * scaler) as i64;
            dst.push(next);
        }
        return Ok(());
    }

    // TODO(edd): fix this. It's copying, which is slowwwwwwwww.
    for v in res.iter() {
        next += *v as i64;
        dst.push(next);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_all_no_values() {
        let mut src: Vec<i64> = vec![];
        let mut dst = vec![];

        // check for error
        encode_all(&mut src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_all_uncompressed() {
        let mut src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        let exp = src.clone();
        encode_all(&mut src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, Encoding::Uncompressed as u8);

        let mut got = vec![];
        decode_all(&dst, &mut got).expect("failed to decode");

        // verify got same values back
        assert_eq!(got, exp);
    }

    #[test]
    fn encode_all_rle() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("no delta positive"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("no delta negative"),
                input: vec![-2398749823764923; 10000],
            },
            Test {
                name: String::from("no delta negative"),
                input: vec![-345632452354; 1000],
            },
            Test {
                name: String::from("delta positive 1"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta positive 2000"),
                input: vec![100, 2100, 4100, 6100, 8100, 10100, 12100, 14100],
            },
            Test {
                name: String::from("delta negative"),
                input: vec![-350, -200, -50],
            },
            Test {
                name: String::from("delta mixed"),
                input: vec![-35000, -5000, 25000, 55000],
            },
            Test {
                name: String::from("delta descending"),
                input: vec![100, 50, 0, -50, -100, -150],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let mut src = test.input.clone();
            let exp = test.input;
            encode_all(&mut src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(&dst[0] >> 4, Encoding::Rle as u8);

            let mut got = vec![];
            decode_all(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    fn encode_all_simple8b() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("positive"),
                input: vec![1, 11, 3124, 123543256, 2398567984273478],
            },
            Test {
                name: String::from("negative"),
                input: vec![-109290, -1234, -123, -12],
            },
            Test {
                name: String::from("mixed"),
                input: vec![-109290, -1234, -123, -12, 0, 0, 0, 1234, 44444, 4444444],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let mut src = test.input.clone();
            let exp = test.input;
            encode_all(&mut src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[0] >> 4, Encoding::Simple8b as u8);

            let mut got = vec![];
            decode_all(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }
}
