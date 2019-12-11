use super::*;

#[test]
fn test_encode_all_no_values() {
    let mut src: Vec<i64> = vec![];
    let mut dst = vec![];

    // check for error
    encode_all(&mut src, &mut dst).expect("failed to encode src");

    // verify encoded no values.
    assert_eq!(dst.to_vec(), vec![]);
}

#[test]
fn test_encode_all_uncompressed() {
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
fn test_encode_all_rle() {
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
            input: vec![-345632452354; 1000],
        },
        Test {
            name: String::from("delta positive"),
            input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
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
fn test_encode_all_simple8b() {
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
