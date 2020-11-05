use std::error::Error;

//
// Adapted from https://github.com/stuartcarnie/rust-encoding
//
const S8B_BIT_SIZE: usize = 60;

// maximum value that can be encoded.
pub const MAX_VALUE: u64 = (1 << 60) - 1;

const NUM_BITS: [[u8; 2]; 14] = [
    [60, 1],
    [30, 2],
    [20, 3],
    [15, 4],
    [12, 5],
    [10, 6],
    [8, 7],
    [7, 8],
    [6, 10],
    [5, 12],
    [4, 15],
    [3, 20],
    [2, 30],
    [1, 60],
];

/// encode packs and binary encodes the provides slice of u64 values using
/// simple8b into the provided vector.
pub fn encode(src: &[u64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    let mut i = 0;
    'next_value: while i < src.len() {
        // try to pack a run of 240 or 120 1s
        let remain = src.len() - i;
        if remain >= 120 {
            let a = if remain >= 240 {
                &src[i..i + 240]
            } else {
                &src[i..i + 120]
            };

            // search for the longest sequence of 1s in a
            let k = a.iter().take_while(|x| **x == 1).count();
            if k == 240 {
                i += 240;
                dst.resize(dst.len() + 8, 0);
                continue;
            } else if k >= 120 {
                i += 120;
                dst.extend_from_slice(&(1u64 << 60).to_be_bytes());
                continue;
            }
        }

        'codes: for (idx, code) in NUM_BITS.iter().enumerate() {
            let (int_n, bit_n) = (code[0] as usize, code[1] as usize);
            if int_n > remain {
                continue;
            }

            let max_val = 1u64 << (bit_n & 0x3f) as u64;
            let mut val = ((idx + 2) << S8B_BIT_SIZE) as u64;
            for (k, in_v) in src[i..].iter().enumerate() {
                if k < int_n {
                    if *in_v >= max_val {
                        continue 'codes;
                    }
                    val |= in_v << ((k * bit_n) as u8 & 0x3f)
                } else {
                    break;
                }
            }
            dst.extend_from_slice(&val.to_be_bytes());
            i += int_n;
            continue 'next_value;
        }
        return Err(From::from("value out of bounds"));
    }
    Ok(())
}

/// decode decodes and unpacks the binary-encoded values stored in src into
/// dst.
pub fn decode(src: &[u8], dst: &mut Vec<u64>) {
    let mut i = 0;
    let mut j = 0;
    let mut buf: [u8; 8] = [0; 8];
    while i < src.len() {
        if dst.len() < j + 240 {
            dst.resize(j + 240, 0); // may need 240 capacity
        }
        buf.copy_from_slice(&src[i..i + 8]);
        j += decode_value(u64::from_be_bytes(buf), &mut dst[j..]);
        i += 8;
    }
    dst.truncate(j);
}

fn decode_value(v: u64, dst: &mut [u64]) -> usize {
    let sel = v >> S8B_BIT_SIZE as u64;
    let mut v = v;
    match sel {
        0 => {
            for i in &mut dst[0..240] {
                *i = 1;
            }
            240
        }
        1 => {
            for i in &mut dst[0..120] {
                *i = 1
            }
            120
        }
        2 => {
            for i in &mut dst[0..60] {
                *i = v & 0x01;
                v >>= 1
            }
            60
        }
        3 => {
            for i in &mut dst[0..30] {
                *i = v & 0x03;
                v >>= 2
            }
            30
        }
        4 => {
            for i in &mut dst[0..20] {
                *i = v & 0x07;
                v >>= 3
            }
            20
        }
        5 => {
            for i in &mut dst[0..15] {
                *i = v & 0x0f;
                v >>= 4
            }
            15
        }
        6 => {
            for i in &mut dst[0..12] {
                *i = v & 0x1f;
                v >>= 5
            }
            12
        }
        7 => {
            for i in &mut dst[0..10] {
                *i = v & 0x3f;
                v >>= 6
            }
            10
        }
        8 => {
            for i in &mut dst[0..8] {
                *i = v & 0x7f;
                v >>= 7
            }
            8
        }
        9 => {
            for i in &mut dst[0..7] {
                *i = v & 0xff;
                v >>= 8
            }
            7
        }
        10 => {
            for i in &mut dst[0..6] {
                *i = v & 0x03ff;
                v >>= 10
            }
            6
        }
        11 => {
            for i in &mut dst[0..5] {
                *i = v & 0x0fff;
                v >>= 12
            }
            5
        }
        12 => {
            dst[0] = v & 0x7fff;
            dst[1] = (v >> 15) & 0x7fff;
            dst[2] = (v >> 30) & 0x7fff;
            dst[3] = (v >> 45) & 0x7fff;
            4
        }
        13 => {
            for i in &mut dst[0..3] {
                *i = v & 0x000f_ffff;
                v >>= 20
            }
            3
        }
        14 => {
            for i in &mut dst[0..2] {
                *i = v & 0x3fff_ffff;
                v >>= 30
            }
            2
        }
        15 => {
            dst[0] = v & 0x0fff_ffff_ffff_ffff;
            1
        }
        _ => 0,
    }
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_encode_no_values() {
        let src = vec![];
        let mut dst = vec![];

        // check for error
        encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), src.len())
    }

    #[test]
    fn test_encode_mixed_sizes() {
        let src = vec![7, 6, 256, 4, 3, 2, 1];

        let mut encoded = vec![];
        let mut decoded = vec![];
        encode(&src, &mut encoded).expect("failed to encode");
        assert_eq!(encoded.len(), 16); // verify vector is truncated.
        decode(&encoded, &mut decoded);
        assert_eq!(decoded.to_vec(), src, "{}", "mixed sizes");
    }

    #[test]
    fn test_encode_mixed_sizes_alt() {
        let src = vec![1, 11, 3124, 123543256, 2398567984273478];

        let mut encoded = vec![];
        let mut decoded = vec![];
        encode(&src, &mut encoded).expect("failed to encode");
        assert_eq!(encoded.len(), 24); // verify vector is truncated.
        decode(&encoded, &mut decoded);
        assert_eq!(decoded.to_vec(), src, "{}", "mixed sizes");
    }

    #[test]
    fn test_encode_too_big() {
        let src = vec![7, 6, 2 << (61 - 1), 4, 3, 2, 1];

        let mut encoded = vec![];
        let result = encode(&src, &mut encoded);
        assert_eq!(result.unwrap_err().to_string(), "value out of bounds");
    }

    #[test]
    fn test_encode() {
        struct Test {
            name: String,
            // TODO(edd): no idea how to store the closure in the struct rather than the
            // result.
            input: Vec<u64>,
        }

        let tests = vec![
            Test {
                name: String::from("1 bit"),
                input: bits(100, 1)(),
            },
            Test {
                name: String::from("2 bit"),
                input: bits(100, 2)(),
            },
            Test {
                name: String::from("3 bit"),
                input: bits(100, 3)(),
            },
            Test {
                name: String::from("4 bit"),
                input: bits(100, 4)(),
            },
            Test {
                name: String::from("5 bit"),
                input: bits(100, 5)(),
            },
            Test {
                name: String::from("6 bit"),
                input: bits(100, 6)(),
            },
            Test {
                name: String::from("7 bit"),
                input: bits(100, 7)(),
            },
            Test {
                name: String::from("8 bit"),
                input: bits(100, 8)(),
            },
            Test {
                name: String::from("10 bit"),
                input: bits(100, 10)(),
            },
            Test {
                name: String::from("12 bit"),
                input: bits(100, 12)(),
            },
            Test {
                name: String::from("15 bit"),
                input: bits(100, 15)(),
            },
            Test {
                name: String::from("20 bit"),
                input: bits(100, 20)(),
            },
            Test {
                name: String::from("30 bit"),
                input: bits(100, 30)(),
            },
            Test {
                name: String::from("60 bit"),
                input: bits(100, 60)(),
            },
            Test {
                name: String::from("240 ones"),
                input: ones(240)(),
            },
        ];

        for test in tests {
            let mut encoded = vec![];
            encode(&test.input, &mut encoded).expect("failed to encode");
            let mut decoded = vec![];
            decode(&encoded, &mut decoded);
            assert_eq!(decoded.to_vec(), test.input, "{}", test.name);
        }

        // Some special cases that are tricky to get into the structs until I figure
        // out how to make `input` a function.

        let mut input = ones(240)();
        input[120] = 5;
        let mut encoded = vec![];
        encode(&input, &mut encoded).expect("failed to encode");
        let mut decoded = vec![];
        decode(&encoded, &mut decoded);
        assert_eq!(decoded.to_vec(), input, "{}", "120 ones");

        input = ones(240)();
        input[119] = 5;

        let mut encoded = vec![];
        encode(&input, &mut encoded).expect("failed to encode");
        let mut decoded = vec![];
        decode(&encoded, &mut decoded);
        assert_eq!(decoded.to_vec(), input, "{}", "119 ones");

        input = ones(241)();
        input[239] = 5;

        let mut encoded = vec![];
        encode(&input, &mut encoded).expect("failed to encode");
        let mut decoded = vec![];
        decode(&encoded, &mut decoded);
        assert_eq!(decoded.to_vec(), input, "{}", "239 ones");
    }

    fn bits(n: u64, bits: u8) -> impl Fn() -> Vec<u64> {
        // move takes ownership of captured variable n
        move || {
            let max = 1 << bits;
            let mut rng: StdRng = SeedableRng::seed_from_u64(231);
            let mut a = Vec::with_capacity(n as usize);
            for i in 0..n {
                let top_bit = (i & 1) << (bits - 1);
                let v = rng.gen_range(0, max) | top_bit;
                assert!(v < max);
                a.push(v);
            }
            a
        }
    }

    fn ones(n: u64) -> impl Fn() -> Vec<u64> {
        move || vec![1; n as usize]
    }
}
