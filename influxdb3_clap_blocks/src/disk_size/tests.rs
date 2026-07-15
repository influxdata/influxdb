use super::*;
use crate::size_units::{GB, MB, TB};

#[test]
fn parses_absolute_sizes_with_unit_suffix() {
    assert_eq!(
        "100mb".parse::<DiskSize>().unwrap().as_num_bytes(),
        100 * MB
    );
    assert_eq!("2gb".parse::<DiskSize>().unwrap().as_num_bytes(), 2 * GB);
    assert_eq!("1tb".parse::<DiskSize>().unwrap().as_num_bytes(), TB);
}

#[test]
fn parsing_is_case_insensitive_and_allows_whitespace() {
    assert_eq!("1TB".parse::<DiskSize>().unwrap().as_num_bytes(), TB);
    assert_eq!("5 gb".parse::<DiskSize>().unwrap().as_num_bytes(), 5 * GB);
    assert_eq!(" 2Gb ".parse::<DiskSize>().unwrap().as_num_bytes(), 2 * GB);
}

#[test]
fn rejects_bare_numbers() {
    assert!("1024".parse::<DiskSize>().is_err());
}

#[test]
fn rejects_percentages() {
    assert!("50%".parse::<DiskSize>().is_err());
}

#[test]
fn accepts_byte_and_kilobyte_units() {
    assert_eq!("1048576b".parse::<DiskSize>().unwrap().as_num_bytes(), MB);
    assert_eq!("1024kb".parse::<DiskSize>().unwrap().as_num_bytes(), MB);
}

#[test]
fn sub_block_sizes_are_an_error() {
    let err = "1000b".parse::<DiskSize>().unwrap_err();
    assert!(err.contains("less than minimum block size"), "{err}");
    assert!("3kb".parse::<DiskSize>().is_err());
    assert!(DiskSize::<4096>::from_bytes(4095).is_err());
}

#[test]
fn overflowing_sizes_are_an_error_not_a_wrap() {
    assert!("99999999999tb".parse::<DiskSize>().is_err());
    assert!("18446744073709551615gb".parse::<DiskSize>().is_err());
}

#[test]
fn rejects_malformed_input() {
    assert!("".parse::<DiskSize>().is_err());
    assert!("gb".parse::<DiskSize>().is_err());
    assert!("-1gb".parse::<DiskSize>().is_err());
    assert!("1.5gb".parse::<DiskSize>().is_err());
}

#[test]
fn parsed_sizes_are_block_aligned() {
    for input in ["1mb", "3mb", "7gb", "2tb"] {
        let size = input.parse::<DiskSize>().unwrap();
        assert_eq!(
            size.as_num_bytes() % 4096,
            0,
            "{input} did not produce a block-aligned size"
        );
    }
}

#[test]
fn from_bytes_floors_to_block_size() {
    assert_eq!(
        DiskSize::<4096>::from_bytes(4096).unwrap().as_num_bytes(),
        4096
    );
    assert_eq!(
        DiskSize::<4096>::from_bytes(4097).unwrap().as_num_bytes(),
        4096
    );
    assert_eq!(
        DiskSize::<4096>::from_bytes(10 * 4096 + 123)
            .unwrap()
            .as_num_bytes(),
        10 * 4096
    );
}

#[test]
fn block_size_is_parameterizable() {
    assert_eq!(DiskSize::<8>::from_bytes(17).unwrap().as_num_bytes(), 16);
    assert_eq!(
        "1000b".parse::<DiskSize<512>>().unwrap().as_num_bytes(),
        512
    );
}
