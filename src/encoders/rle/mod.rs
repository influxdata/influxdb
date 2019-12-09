use integer_encoding::*;

/// encode encodes the value v, delta and count into dst.
///
/// v should be the first element of a sequence, delta the difference that each
/// value in the sequence differs by, and count the total number of values in the
/// sequence.
fn encode(v: u64, delta: u64, count: u64, dst: &mut Vec<u8>) {
    let max_var_int_size = 10; // max number of bytes needed to store var int

    // Keep a byte back for the scaler.
    dst.push(0);
    let mut n = 1;
    // write the first value in as a byte array.
    dst.extend_from_slice(&v.to_be_bytes());
    n += 8;

    // check the first delta's divisor. All deltas are the same, so we only
    // need to check the first one.
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

/// decode decodes an RLE encoded slice into the destination vector.
fn decode(src: &[u8], dst: &mut Vec<u64>) -> Result<(), &'static str> {
    if src.len() < 9 {
        return Err("not enough data to decode using RLE");
    }

    // calculate the scaler from the lower 4 bits of the first byte.
    let scaler = 10_u64.pow((src[0] & 0xF) as u32);
    let mut i = 1;

    // TODO(edd): this should be possible to do in-place without copy.
    let mut a: [u8; 8] = [0; 8];
    a.copy_from_slice(&src[i..i + 8]);
    let mut first = u64::from_be_bytes(a);
    i += 8;
    let (mut delta, n) = u64::decode_var(&src[i..]);
    if n <= 0 {
        return Err("unable to decode delta");
    }
    i += n;
    delta *= scaler;

    let (count, n) = usize::decode_var(&src[i..]);
    if n <= 0 {
        return Err("unable to decode count");
    }

    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }

    for _ in 0..count {
        dst.push(first);
        first += delta;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_rle() {
        let exp = vec![100, 2100, 4100, 6100, 8100, 10100, 12100, 14100];
        let mut dst = Vec::with_capacity(100);
        encode(100, 2000, 8, &mut dst);

        let mut got = Vec::with_capacity(0);
        decode(&dst, &mut got).expect("failed to RLE decode");
        assert_eq!(got, exp);
    }

    #[test]
    fn test_encode_rle_no_cap() {
        let exp = vec![100, 2100, 4100, 6100, 8100, 10100, 12100, 14100];
        let mut dst = vec![];
        encode(100, 2000, 8, &mut dst);

        let mut got = Vec::with_capacity(0);
        decode(&dst, &mut got).expect("failed to RLE decode");
        assert_eq!(got, exp);
    }

    #[test]
    fn test_encode_rle_small() {
        let exp = vec![22222222, 22222222];
        let mut dst = vec![];
        encode(22222222, 0, 2, &mut dst);

        let mut got = Vec::with_capacity(0);
        decode(&dst, &mut got).expect("failed to RLE decode");
        assert_eq!(got, exp);
    }
}
