use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[test]
fn test_encode_no_values() {
    let src = vec![];
    let mut dst = vec![];

    // check for error
    let v = encode_all(&src, &mut dst).expect("failed to encode src");

    // verify encoded no values.
    assert_eq!(v.to_vec(), src)
}

#[test]
fn test_encode_mixed_sizes() {
    let src = vec![7, 6, 256, 4, 3, 2, 1];

    let mut encoded = vec![0; src.len() as usize];
    let encoded = encode_all(&src, &mut encoded).expect("failed to encode");
    let mut decoded = vec![0; src.len() as usize];
    let decoded = decode_all(&encoded, &mut decoded).expect("failed to decode");
    assert_eq!(decoded.to_vec(), src, "{}", "mixed sizes");
}

#[test]
fn test_encode_too_big() {
    let src = vec![7, 6, 2 << 61 - 1, 4, 3, 2, 1];

    let mut encoded = vec![0; src.len() as usize];
    let e = encode_all(&src, &mut encoded).expect_err("encoding did not fail");
    assert_eq!(e, "value out of bounds");
}

#[test]
fn test_encode_all() {
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
        let mut encoded = vec![0; test.input.len() as usize];
        let encoded = encode_all(&test.input, &mut encoded).expect("failed to encode");
        let mut decoded = vec![0; test.input.len() as usize];
        let decoded = decode_all(&encoded, &mut decoded).expect("failed to decode");
        assert_eq!(decoded.to_vec(), test.input, "{}", test.name);
    }

    // Some special cases that are tricky to get into the structs until I figure
    // out how to make `input` a function.

    let mut input = ones(240)();
    input[120] = 5;

    let mut encoded = vec![0; input.len() as usize];
    let encoded = encode_all(&input, &mut encoded).expect("failed to encode");
    let mut decoded = vec![0; input.len() as usize];
    let decoded = decode_all(&encoded, &mut decoded).expect("failed to decode");
    assert_eq!(decoded.to_vec(), input, "{}", "120 ones");

    input = ones(240)();
    input[119] = 5;

    let mut encoded = vec![0; input.len() as usize];
    let encoded = encode_all(&input, &mut encoded).expect("failed to encode");
    let mut decoded = vec![0; input.len() as usize];
    let decoded = decode_all(&encoded, &mut decoded).expect("failed to decode");
    assert_eq!(decoded.to_vec(), input, "{}", "119 ones");

    input = ones(241)();
    input[239] = 5;

    let mut encoded = vec![0; input.len() as usize];
    let encoded = encode_all(&input, &mut encoded).expect("failed to encode");
    let mut decoded = vec![0; input.len() as usize];
    let decoded = decode_all(&encoded, &mut decoded).expect("failed to decode");
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
