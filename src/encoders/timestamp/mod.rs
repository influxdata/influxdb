use super::{rle, simple8b};
use std::error::Error;

/// Encoding describes the type of encoding used by an encoded timestamp block.
enum Encoding {
    Uncompressed = 0,
    Simple8b = 1,
    Rle = 2,
}

fn encode_all<'a>(src: &mut Vec<i64>, dst: &'a mut Vec<u8>) -> Result<(), Box<Error>> {
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
            rle::encode_all(deltas[0], deltas[1], deltas.len() as u64, dst);
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

/// i64_to_u64_vector converts a Vec<i64> to Vec<u64>.
/// TODO(edd): this is expensive as it copies. There are cheap
/// but unsafe alternatives to look into such as std::mem::transmute
fn i64_to_u64_vector(src: &[i64]) -> Vec<u64> {
    src.into_iter().map(|x| *x as u64).collect::<Vec<u64>>()
}

/// u64_to_i64_vector converts a Vec<u64> to Vec<i64>.
/// TODO(edd): this is expensive as it copies. There are cheap
/// but unsafe alternatives to look into such as std::mem::transmute
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.into_iter().map(|x| *x as i64).collect::<Vec<i64>>()
}

fn decode_all<'a>(src: &[u8], dst: &'a mut Vec<i64>) -> Result<(), Box<Error>> {
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

fn decode_uncompressed(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
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

fn decode_rle(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
    let mut res = vec![];
    if let Err(e) = rle::decode_all(&src, &mut res) {
        return Err(e);
    }
    // TODO(edd): fix this. It's copying, which is slowwwwwwwww.
    for v in res.iter() {
        dst.push(*v as i64);
    }
    Ok(())
}

fn decode_simple8b(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
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
mod tests;
