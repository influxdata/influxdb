use std::error::Error;

// SENTINEL is used to terminate a float-encoded block. A sentinel marker value
// is useful because blocks do not always end aligned to bytes, and spare empty
// bits can otherwise have undesirable semantic meaning.
const SENTINEL: u64 = 0x7ff8_0000_0000_00ff; // in the quiet NaN range.
const SENTINEL_INFLUXDB: u64 = 0x7ff8_0000_0000_0001; // legacy NaN value used by InfluxDB

fn is_sentinel_f64(v: f64, sentinel: u64) -> bool {
    v.to_bits() == sentinel
}
fn is_sentinel_u64(v: u64, sentinel: u64) -> bool {
    v == sentinel
}

/// encode encodes a vector of floats into dst.
///
/// The encoding used is equivalent to the encoding of floats in the Gorilla
/// paper. Each subsequent value is compared to the previous and the XOR of the
/// two is determined. Leading and trailing zero bits are then analysed and
/// representations based on those are stored.
#[allow(clippy::many_single_char_names)]
pub fn encode(src: &[f64], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    dst.clear(); // reset buffer.
    if src.is_empty() {
        return Ok(());
    }
    if dst.capacity() < 9 {
        dst.reserve_exact(9 - dst.capacity()); // room for encoding type, block
                                               // size and a value
    }

    // write encoding type
    let mut n = 8; // N.B, this is the number of bits written
    dst.push((1 << 4) as u8); // write compression type

    // write the first value into the block
    let first = src[0];
    let mut prev = first.to_bits();
    dst.extend_from_slice(&prev.to_be_bytes());
    n += 64;

    let (mut prev_leading, mut prev_trailing) = (!0u64, 0u64);
    // encode remaining values
    for i in 1..=src.len() {
        let x;
        if i < src.len() {
            x = src[i];
            if is_sentinel_f64(x, SENTINEL) {
                return Err(From::from("unsupported value"));
            }
        } else {
            x = f64::from_bits(SENTINEL);
        }

        let cur = x.to_bits();
        let v_delta = cur ^ prev;
        if v_delta == 0 {
            n += 1; // write a single zero bit, nothing else to do
            prev = cur;
            continue;
        }

        while n >> 3 >= dst.len() {
            dst.push(0); // make room
        }

        // set the current bit of the current byte to indicate we are writing a
        // delta value to the output
        // n&7 - current bit in current byte
        // n>>3 - current byte
        dst[n >> 3] |= 128 >> (n & 7); // set the current bit of the current byte
        n += 1;

        // next, write the delta to the output
        let mut leading = v_delta.leading_zeros() as u64;
        let trailing = v_delta.trailing_zeros() as u64;

        // prevent overflow by restricting number of leading zeros to 31
        leading &= 0b0001_1111;

        // a minimum of two further bits will be required
        if (n + 2) >> 3 >= dst.len() {
            dst.push(0);
        }

        if prev_leading != !0u64 && leading >= prev_leading && trailing >= prev_trailing {
            n += 1; // write leading bit

            let l = 64 - prev_leading - prev_trailing; // none-zero bit count
            while (n + 1) >> 3 >= dst.len() {
                dst.push(0); // grow to accommodate bits.
            }

            // the full value
            let v = (v_delta >> prev_trailing) << (64 - l); // l least significant bits of v
            let m = (n & 7) as u64; // current bit in current byte
            let mut written = 0u64;
            if m > 0 {
                // the current byte has not been completely filled
                written = if l < 8 - m { l } else { 8 - m };
                let mask = v >> 56; // move 8 MSB to 8 LSB
                dst[n >> 3] |= (mask >> m) as u8;
                n += written as usize;

                if l - written == 0 {
                    prev = cur;
                    continue;
                }
            }

            let vv = v << written; // move written bits out of the way
            while (n >> 3) + 8 >= dst.len() {
                dst.push(0);
            }
            // TODO(edd): maybe this can be optimised?
            let k = n >> 3;
            let vv_bytes = &vv.to_be_bytes();
            dst[k..k + 8].clone_from_slice(&vv_bytes[0..(k + 8 - k)]);

            n += (l - written) as usize;
        } else {
            prev_leading = leading;
            prev_trailing = trailing;

            // set a single bit to indicate a value will follow
            dst[n >> 3] |= 128 >> (n & 7); // set the current bit on the current byte
            n += 1;

            // write 5 bits of leading
            if (n + 5) >> 3 >= dst.len() {
                dst.push(0);
            }

            // see if there is enough room left in current byte for the 5 bits.
            let mut m = n & 7;
            let mut l = 5usize;
            let mut v = leading << 59; // 5 LSB of leading
            let mut mask = v >> 56; // move 5 MSB to 8 LSB

            if m <= 3 {
                // 5 bits fit in current byte
                dst[n >> 3] |= (mask >> m) as u8;
                n += l;
            } else {
                // not enough bits available in current byte
                let written = 8 - m;
                dst[n >> 3] |= (mask >> m) as u8; // some of mask will get lost
                n += written;

                // next the lost part of mask needs to be written into the next byte
                mask = v << written; // move already written bits out the way
                mask >>= 56;

                m = n & 7; // new current bit
                dst[n >> 3] |= (mask >> m) as u8;
                n += l - written;
            }

            // Note that if leading == trailing == 0, then sig_bits == 64. But
            // that value doesn't actually fit into the 6 bits we have. However,
            // we never need to encode 0 significant bits, since that would put
            // us in the other case (v_delta == 0). So instead we write out a 0
            // and adjust it back to 64 on unpacking.
            let sig_bits = 64 - leading - trailing;
            if (n + 6) >> 3 >= dst.len() {
                dst.push(0);
            }

            m = n & 7;
            l = 6;
            v = sig_bits << 58; // move 6 LSB of sig_bits to MSB
            let mut mask = v >> 56; // move 6 MSB to 8 LSB
            if m <= 2 {
                dst[n >> 3] |= (mask >> m) as u8; // the 6 bits fit in the current byte
                n += l;
            } else {
                let written = 8 - m;
                dst[n >> 3] |= (mask >> m) as u8; // fill rest of current byte
                n += written;

                // next, write the lost part of mask into the next byte
                mask = v << written;
                mask >>= 56;

                m = n & 7; // recompute current bit to write
                dst[n >> 3] |= (mask >> m) as u8;
                n += l - written;
            }

            // write final value
            m = n & 7;
            l = sig_bits as usize;
            v = (v_delta >> trailing) << (64 - l); // move l LSB into MSB
            while (n + l) >> 3 >= dst.len() {
                dst.push(0);
            }

            let mut written = 0usize;
            if m > 0 {
                // current byte not full
                written = if l < 8 - m { l } else { 8 - m };
                mask = v >> 56; // move 8 MSB to 8 LSB
                dst[n >> 3] |= (mask >> m) as u8;
                n += written;

                if l - written == 0 {
                    prev = cur;
                    continue;
                }
            }

            // shift remaining bits and write out
            let vv = v << written; // remove bits written in previous byte
            while (n >> 3) + 8 >= dst.len() {
                dst.push(0);
            }

            // TODO(edd): maybe this can be optimised?
            let k = n >> 3;
            let vv_bytes = &vv.to_be_bytes();
            dst[k..k + 8].clone_from_slice(&vv_bytes[0..(k + 8 - k)]);
            n += l - written;
        }
        prev = cur;
    }

    let mut length = n >> 3;
    if n & 7 > 0 {
        length += 1;
    }
    dst.truncate(length);
    Ok(())
}

// BIT_MASK contains a lookup table where the index is the number of bits
// and the value is a mask. The table is always read by ANDing the index
// with 0x3f, such that if the index is 64, position 0 will be read, which
// is a 0xffffffffffffffff, thus returning all bits.
//
// 00 = 0xffffffffffffffff
// 01 = 0x0000000000000001
// 02 = 0x0000000000000003
// 03 = 0x0000000000000007
// ...
// 62 = 0x3fffffffffffffff
// 63 = 0x7fffffffffffffff
//
// TODO(edd): figure out how to generate this.
const BIT_MASK: [u64; 64] = [
    0xffff_ffff_ffff_ffff,
    0x0001,
    0x0003,
    0x0007,
    0x000f,
    0x001f,
    0x003f,
    0x007f,
    0x00ff,
    0x01ff,
    0x03ff,
    0x07ff,
    0x0fff,
    0x1fff,
    0x3fff,
    0x7fff,
    0xffff,
    0x0001_ffff,
    0x0003_ffff,
    0x0007_ffff,
    0x000f_ffff,
    0x001f_ffff,
    0x003f_ffff,
    0x007f_ffff,
    0x00ff_ffff,
    0x01ff_ffff,
    0x03ff_ffff,
    0x07ff_ffff,
    0x0fff_ffff,
    0x1fff_ffff,
    0x3fff_ffff,
    0x7fff_ffff,
    0xffff_ffff,
    0x0001_ffff_ffff,
    0x0003_ffff_ffff,
    0x0007_ffff_ffff,
    0x000f_ffff_ffff,
    0x001f_ffff_ffff,
    0x003f_ffff_ffff,
    0x007f_ffff_ffff,
    0x00ff_ffff_ffff,
    0x01ff_ffff_ffff,
    0x03ff_ffff_ffff,
    0x07ff_ffff_ffff,
    0x0fff_ffff_ffff,
    0x1fff_ffff_ffff,
    0x3fff_ffff_ffff,
    0x7fff_ffff_ffff,
    0xffff_ffff_ffff,
    0x0001_ffff_ffff_ffff,
    0x0003_ffff_ffff_ffff,
    0x0007_ffff_ffff_ffff,
    0x000f_ffff_ffff_ffff,
    0x001f_ffff_ffff_ffff,
    0x003f_ffff_ffff_ffff,
    0x007f_ffff_ffff_ffff,
    0x00ff_ffff_ffff_ffff,
    0x01ff_ffff_ffff_ffff,
    0x03ff_ffff_ffff_ffff,
    0x07ff_ffff_ffff_ffff,
    0x0fff_ffff_ffff_ffff,
    0x1fff_ffff_ffff_ffff,
    0x3fff_ffff_ffff_ffff,
    0x7fff_ffff_ffff_ffff,
];

/// decode decodes the provided slice of bytes into a vector of f64 values.
pub fn decode(src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error>> {
    decode_with_sentinel(src, dst, SENTINEL)
}

/// decode_influxdb decodes the provided slice of bytes, which must have been
/// encoded into a TSM file via InfluxDB's encoder.
///
/// TODO(edd): InfluxDB uses a different  sentinel value to terminate a block
/// than we chose to use for the float decoder. As we settle on a story around
/// compression of f64 blocks we may be able to clean this API and not have
/// multiple methods.
pub fn decode_influxdb(src: &[u8], dst: &mut Vec<f64>) -> Result<(), Box<dyn Error>> {
    decode_with_sentinel(src, dst, SENTINEL_INFLUXDB)
}

/// decode decodes a slice of bytes into a vector of floats.
#[allow(clippy::many_single_char_names)]
#[allow(clippy::useless_let_if_seq)]
fn decode_with_sentinel(
    src: &[u8],
    dst: &mut Vec<f64>,
    sentinel: u64,
) -> Result<(), Box<dyn Error>> {
    if src.len() < 9 {
        return Ok(());
    }

    let mut i = 1; // skip first byte as it's the encoding, which is always gorilla
    let mut buf: [u8; 8] = [0; 8];

    // the first decoded value
    buf.copy_from_slice(&src[i..i + 8]);
    let mut val = u64::from_be_bytes(buf);
    i += 8;
    dst.push(f64::from_bits(val));

    // decode the rest of the values
    let mut br_cached_val;
    let mut br_valid_bits;

    // Refill br_cached_value, reading up to 8 bytes from b, returning the new
    // values for the cached value, the valid bits and the number of bytes read.
    let mut refill_cache = |i: usize| -> Result<(u64, u8, usize), Box<dyn Error>> {
        let remaining_bytes = src.len() - i;
        if remaining_bytes >= 8 {
            // read 8 bytes directly
            buf.copy_from_slice(&src[i..i + 8]);
            return Ok((u64::from_be_bytes(buf), 64, 8));
        } else if remaining_bytes > 0 {
            let mut br_cached_val = 0u64;
            let br_valid_bits = (remaining_bytes * 8) as u8;
            let mut n = 0;
            for v in src.iter().skip(i) {
                br_cached_val = (br_cached_val << 8) | *v as u64;
                n += 1;
            }
            br_cached_val = br_cached_val.rotate_right(br_valid_bits as u32);
            return Ok((br_cached_val, br_valid_bits, n));
        }
        Err(From::from("unexpected end of block"))
    };

    // TODO(edd): I found it got complicated quickly when trying to use Ref to
    // mutate br_cached_val, br_valid_bits and I directly in the closure, so for
    // now we will just mutate copies and re-assign...
    match refill_cache(i) {
        Ok(res) => {
            br_cached_val = res.0;
            br_valid_bits = res.1;
            i += res.2;
        }
        Err(e) => return Err(e),
    }

    let mut trailing_n = 0u8;
    let mut meaningful_n = 64u8;

    loop {
        if br_valid_bits == 0 {
            match refill_cache(i) {
                Ok(res) => {
                    br_cached_val = res.0;
                    br_valid_bits = res.1;
                    i += res.2;
                }
                Err(e) => return Err(e),
            }
        }

        // read control bit 0.
        br_valid_bits -= 1;
        br_cached_val = br_cached_val.rotate_left(1);
        if br_cached_val & 1 == 0 {
            dst.push(f64::from_bits(val));
            continue;
        }

        if br_valid_bits == 0 {
            match refill_cache(i) {
                Ok(res) => {
                    br_cached_val = res.0;
                    br_valid_bits = res.1;
                    i += res.2;
                }
                Err(e) => return Err(e),
            }
        }

        // read control bit 1.
        br_valid_bits -= 1;
        br_cached_val = br_cached_val.rotate_left(1);
        if br_cached_val & 1 > 0 {
            // read 5 bits for leading zero count and 6 bits for the meaningful data count
            let leading_trailing_bit_count = 11;
            let mut lm_bits = 0u64; // leading + meaningful data counts
            if br_valid_bits >= leading_trailing_bit_count {
                // decode 5 bits leading + 6 bits meaningful for a total of 11 bits
                br_valid_bits -= leading_trailing_bit_count;
                br_cached_val = br_cached_val.rotate_left(leading_trailing_bit_count as u32);
                lm_bits = br_cached_val;
            } else {
                let mut bits_01 = 11u8;
                if br_valid_bits > 0 {
                    bits_01 -= br_valid_bits;
                    lm_bits = br_cached_val.rotate_left(11);
                }

                match refill_cache(i) {
                    Ok(res) => {
                        br_cached_val = res.0;
                        br_valid_bits = res.1;
                        i += res.2;
                    }
                    Err(e) => return Err(e),
                }

                br_cached_val = br_cached_val.rotate_left(bits_01 as u32);
                br_valid_bits -= bits_01;
                lm_bits &= !BIT_MASK[(bits_01 & 0x3f) as usize];
                lm_bits |= br_cached_val & BIT_MASK[(bits_01 & 0x3f) as usize];
            }

            lm_bits &= 0x7ff;
            let leading_n = (lm_bits >> 6) as u8 & 0x1f; // 5 bits leading
            meaningful_n = (lm_bits & 0x3f) as u8; // 6 bits meaningful
            if meaningful_n > 0 {
                trailing_n = 64 - leading_n - meaningful_n;
            } else {
                // meaningful_n == 0 is a special case, such that all bits are meaningful
                trailing_n = 0;
                meaningful_n = 64;
            }
        }

        let mut s_bits = 0u64; // significant bits
        if br_valid_bits >= meaningful_n {
            br_valid_bits -= meaningful_n;
            br_cached_val = br_cached_val.rotate_left(meaningful_n as u32);
            s_bits = br_cached_val;
        } else {
            let mut m_bits = meaningful_n;
            if br_valid_bits > 0 {
                m_bits -= br_valid_bits;
                s_bits = br_cached_val.rotate_left(meaningful_n as u32);
            }

            match refill_cache(i) {
                Ok(res) => {
                    br_cached_val = res.0;
                    br_valid_bits = res.1;
                    i += res.2;
                }
                Err(e) => return Err(e),
            }

            br_cached_val = br_cached_val.rotate_left(m_bits as u32);
            br_valid_bits = br_valid_bits.wrapping_sub(m_bits);
            s_bits &= !BIT_MASK[(m_bits & 0x3f) as usize];
            s_bits |= br_cached_val & BIT_MASK[(m_bits & 0x3f) as usize];
        }
        s_bits &= BIT_MASK[(meaningful_n & 0x3f) as usize];
        val ^= s_bits << (trailing_n & 0x3f);

        // check for sentinel value
        if is_sentinel_u64(val, sentinel) {
            break;
        }
        dst.push(f64::from_bits(val));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
#[allow(clippy::excessive_precision)] // TODO: Audit test values for truncation
mod tests {
    use test_helpers::approximately_equal;

    #[test]
    fn encode_no_values() {
        let src: Vec<f64> = vec![];
        let mut dst = vec![];

        // check for error
        super::encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        let exp: Vec<u8> = Vec::new();
        assert_eq!(dst.to_vec(), exp);
    }

    #[test]
    fn encode_special_values() {
        let src: Vec<f64> = vec![
            100.0,
            222.12,
            f64::from_bits(0x7ff8000000000001), // Go representation of signalling NaN
            45.324,
            std::f64::NAN,
            2453.023,
            -1234.235312132,
            std::f64::INFINITY,
            std::f64::NEG_INFINITY,
            9123419329123.1234,
            f64::from_bits(0x7ff0000000000002), // Prometheus stale NaN
            -19292929929292929292.22,
            -0.0000000000000000000000000092,
        ];
        let mut dst = vec![];

        // check for error
        super::encode(&src, &mut dst).expect("failed to encode src");

        let mut got = vec![];
        super::decode(&dst, &mut got).expect("failed to decode");

        // Verify decoded values.
        assert_eq!(got.len(), src.len());

        for (i, v) in got.iter().enumerate() {
            if v.is_nan() || v.is_infinite() {
                assert_eq!(src[i].to_bits(), v.to_bits());
            } else {
                assert!(approximately_equal(src[i], *v));
            }
        }
    }

    #[test]
    fn encode() {
        struct Test {
            name: String,
            input: Vec<f64>,
        }

        let tests = vec![
            Test {
                name: String::from("from reference paper"),
                input: vec![12.0, 12.0, 24.0, 13.0, 24.0, 24.0, 24.0, 23.0],
            },
            Test {
                name: String::from("failed in previous implementation"),
                input: vec![
                    -3.8970913068231994e+307,
                    -9.036931257783943e+307,
                    1.7173073833490201e+308,
                    -9.312369166661538e+307,
                    -2.2435523083555231e+307,
                    1.4779121287289644e+307,
                    1.771273431601434e+308,
                    8.140360378221364e+307,
                    4.783405048208089e+307,
                    -2.8044680049605344e+307,
                    4.412915337205696e+307,
                    -1.2779380602005046e+308,
                    1.6235802318921885e+308,
                    -1.3402901846299688e+307,
                    1.6961015582104055e+308,
                    -1.067980796435633e+308,
                    -3.02868987458268e+307,
                    1.7641793640790284e+308,
                    1.6587191845856813e+307,
                    -1.786073304985983e+308,
                    1.0694549382051123e+308,
                    3.5635180996210295e+307,
                ],
            },
            Test {
                name: String::from("previous example as natural numbers"),
                input: vec![
                    -38970913068231994.0,
                    -9036931257783943.0,
                    171730738334902010.0,
                    -9312369166661538.0,
                    -22435523083555231.0,
                    14779121287289644.0,
                    17712734316014340.0,
                    8140360378221364.0,
                    4783405048208089.0,
                    -28044680049605344.0,
                    4412915337205696.0,
                    -127793806020050460.0,
                    162358023189218850.0,
                    -13402901846299688.0,
                    169610155821040550.0,
                    -10679807964356330.0,
                    -302868987458268.0,
                    176417936407902840.0,
                    16587191845856813.0,
                    -17860733049859830.0,
                    106945493820511230.0,
                    35635180996210295.0,
                ],
            },
            Test {
                name: String::from("similar values"),
                input: vec![
                    6.00065e+06,
                    6.000656e+06,
                    6.000657e+06,
                    6.000659e+06,
                    6.000661e+06,
                ],
            },
            Test {
                name: String::from("two hours data"),
                input: vec![
                    761.0, 727.0, 763.0, 706.0, 700.0, 679.0, 757.0, 708.0, 739.0, 707.0, 699.0,
                    740.0, 729.0, 766.0, 730.0, 715.0, 705.0, 693.0, 765.0, 724.0, 799.0, 761.0,
                    737.0, 766.0, 756.0, 719.0, 722.0, 801.0, 747.0, 731.0, 742.0, 744.0, 791.0,
                    750.0, 759.0, 809.0, 751.0, 705.0, 770.0, 792.0, 727.0, 762.0, 772.0, 721.0,
                    748.0, 753.0, 744.0, 716.0, 776.0, 659.0, 789.0, 766.0, 758.0, 690.0, 795.0,
                    770.0, 758.0, 723.0, 767.0, 765.0, 693.0, 706.0, 681.0, 727.0, 724.0, 780.0,
                    678.0, 696.0, 758.0, 740.0, 735.0, 700.0, 742.0, 747.0, 752.0, 734.0, 743.0,
                    732.0, 746.0, 770.0, 780.0, 710.0, 731.0, 712.0, 712.0, 741.0, 770.0, 770.0,
                    754.0, 718.0, 670.0, 775.0, 749.0, 795.0, 756.0, 741.0, 787.0, 721.0, 745.0,
                    782.0, 765.0, 780.0, 811.0, 790.0, 836.0, 743.0, 858.0, 739.0, 762.0, 770.0,
                    752.0, 763.0, 795.0, 792.0, 746.0, 786.0, 785.0, 774.0, 786.0, 718.0,
                ],
            },
            Test {
                name: String::from("identical values"),
                input: vec![12123.1234; 1000],
            },
            Test {
                name: String::from("1000 real CPU values"),
                input: vec![
                    11.286653185035389,
                    3.7310629773381745,
                    1.6102858569466982,
                    1.691305437233776,
                    1.8957345971563981,
                    3.625453181647706,
                    10.073740782402199,
                    7.99398571607568,
                    4.598130841121495,
                    5.293527345709985,
                    6.247661803217359,
                    4.296777416937297,
                    1.3373328333958254,
                    1.5998000249968753,
                    3.0139394700489763,
                    2.0558185895838523,
                    3.18630513557416,
                    4.133882852504059,
                    3.3033033033033035,
                    3.6824366496067906,
                    2.9378672334041753,
                    2.112764095511939,
                    1.5896858179997497,
                    5.252626313156578,
                    2.908137793310035,
                    3.1738098213170063,
                    3.3120859892513437,
                    2.189415738771425,
                    2.9650944576504443,
                    2.826195219123506,
                    2.618391380606364,
                    2.739897410233955,
                    2.886056971514243,
                    2.25140712945591,
                    3.5843636817784437,
                    2.7236381809095453,
                    3.5138176816306115,
                    3.5348488633524857,
                    1.5920772220132882,
                    2.5954579485899676,
                    2.4918607563235664,
                    1.9355644355644355,
                    2.240580798598072,
                    1.8293446936474127,
                    1.1938813580400447,
                    3.0753844230528817,
                    3.2399299474605954,
                    5.420326223337516,
                    6.535540893813021,
                    5.47158026233604,
                    2.55,
                    2.0336429826763744,
                    2.638489433537577,
                    2.8251400124455506,
                    2.3505876469117277,
                    2.9632408102025507,
                    2.7243189202699325,
                    1.712285964254468,
                    1.6506189821182944,
                    1.1495689116581282,
                    1.4140908522087348,
                    1.8733608092918697,
                    2.13696575856036,
                    3.0644152595372107,
                    3.649087728067983,
                    3.649087728067983,
                    2.4799599198396796,
                    3.4676312835225147,
                    2.9992501874531365,
                    2.8371453568303964,
                    3.6660389202762085,
                    3.5487485991781846,
                    3.8509627406851714,
                    4.373981701967665,
                    3.8153615211408556,
                    2.3548467480687765,
                    2.575321915239405,
                    1.4267834793491865,
                    1.2625,
                    1.2353381582231096,
                    3.055243795984537,
                    3.6958155850663994,
                    2.0645645645645647,
                    1.7491254372813594,
                    1.8865567216391803,
                    4.563640910227557,
                    3.6805207811717575,
                    3.3983008495752123,
                    3.6117381489841986,
                    2.832650018635855,
                    1.0390585878818228,
                    3.930131004366812,
                    5.608531994981179,
                    6.253900886281363,
                    5.43640897755611,
                    2.4558326024307733,
                    2.272727272727273,
                    4.708829054477144,
                    3.233458177278402,
                    2.9246344206974126,
                    2.651325662831416,
                    2.6993251687078232,
                    3.994990607388854,
                    2.220558882235529,
                    1.275797373358349,
                    2.0367362239160314,
                    2.8375,
                    1.676257192894671,
                    2.237779722465308,
                    2.135098014733425,
                    1.2259194395796849,
                    1.2489072061945798,
                    1.2503125781445361,
                    1.3375,
                    1.3413563996489908,
                    1.3102071375093587,
                    1.1620642259152818,
                    1.6577340147077153,
                    2.6081504702194356,
                    1.9233170975396527,
                    3.6754594324290535,
                    1.936773709858803,
                    2.766649974962444,
                    2.3833416959357754,
                    2.1663346613545817,
                    2.722957781663752,
                    1.5119330251155816,
                    2.075,
                    2.340132649230384,
                    1.3647176662075873,
                    2.5884706765036887,
                    3.3445231878652244,
                    4.001505268439538,
                    1.9852665751030092,
                    2.9267679939706066,
                    2.8447204968944098,
                    1.9831806200577382,
                    2.695619618120554,
                    3.9003115264797508,
                    2.9944640161046805,
                    2.6381284221005474,
                    2.651657285803627,
                    1.8745313671582104,
                    1.5650431951921873,
                    1.6974538192710933,
                    1.7769991240145164,
                    3.06809678223996,
                    3.730128927275003,
                    4.101221640488657,
                    3.045112781954887,
                    4.24037134612972,
                    3.2709113607990012,
                    1.546713234376949,
                    3.7937019588395735,
                    1.639344262295082,
                    1.1609037573336662,
                    0.7003501750875438,
                    3.4275706780085065,
                    4.846587351283657,
                    8.203907815631263,
                    18.056599048334586,
                    10.340050377833753,
                    4.5375,
                    9.898889027587067,
                    3.705564627559352,
                    4.7613112302131375,
                    7.604467310829464,
                    5.090230242688239,
                    3.286915067118304,
                    4.54602223054827,
                    0.9040247678018576,
                    2.383955600403633,
                    2.8744889109156238,
                    6.1522945032778615,
                    5.16641828117238,
                    5.089218396582056,
                    5.886028492876781,
                    7.0792017070415465,
                    5.167894145549869,
                    2.3025501361723197,
                    0.8625,
                    1.0116148370176097,
                    0.5875734466808351,
                    0.703694395576778,
                    0.7711442786069652,
                    1.587896974243561,
                    3.3358509566968784,
                    1.9875,
                    2.5736203909923288,
                    0.7769423558897243,
                    2.013506753376688,
                    0.7248187953011747,
                    0.7130347760820616,
                    0.8994378513429107,
                    0.7498125468632841,
                    0.7125890736342043,
                    0.9572994079858924,
                    0.6337765626941717,
                    0.8108782435129741,
                    0.7625953244155519,
                    6.323510813203491,
                    4.336989032901296,
                    3.547782635852592,
                    3.8667830859423726,
                    3.95742016280526,
                    1.7510944340212633,
                    0.9375,
                    2.3729236917697016,
                    4.491017964071856,
                    4.435836561289516,
                    5.663939584644431,
                    1.8381893209953732,
                    1.1118051217988758,
                    2.2338699613128665,
                    2.1189081391000872,
                    1.4509068167604753,
                    1.2650300601202404,
                    1.347305389221557,
                    2.108169155477475,
                    3.7398373983739837,
                    3.3872976338729766,
                    1.1,
                    2.059925093632959,
                    1.4130298862073278,
                    1.375171896487061,
                    1.5011258443832876,
                    1.7312758750470456,
                    1.0823587957203284,
                    1.1912225705329154,
                    1.6826623457559517,
                    2.3415977961432506,
                    1.2849301397205588,
                    1.7127140892611576,
                    2.01325497061398,
                    2.049487628092977,
                    1.7157169693174703,
                    1.410736579275905,
                    1.3989507869098177,
                    1.4273193940152749,
                    1.4474669328674818,
                    1.4509068167604753,
                    1.5257628814407203,
                    1.6370907273181705,
                    1.9113054341036853,
                    1.3625,
                    1.2266866942045311,
                    1.6485575121768452,
                    1.311680199875078,
                    1.0628985869701137,
                    1.11236095488064,
                    1.2996750812296927,
                    1.4398397395768123,
                    1.6344354335620712,
                    2.364568997873139,
                    2.7979015738196353,
                    2.4262131065532766,
                    2.11065317846884,
                    2.8639319659829914,
                    1.5248093988251468,
                    2.0497437820272464,
                    1.4380392647242717,
                    1.4507253626813408,
                    1.50018752344043,
                    1.3238416385662546,
                    1.6741629185407296,
                    2.3517638228671505,
                    1.6870782304423895,
                    1.85,
                    1.226379677136779,
                    1.536155863619333,
                    2.5541504945536495,
                    1.963727329580988,
                    2.543640897755611,
                    2.276138069034517,
                    1.4133833646028768,
                    2.4362818590704647,
                    1.7875,
                    1.400525196948856,
                    1.0990383414512301,
                    1.625,
                    2.3744063984004,
                    1.1126390798849857,
                    1.7883941970985493,
                    1.5121219695076231,
                    1.4126765845730715,
                    1.136789506558401,
                    1.2384288216162123,
                    1.1376422052756594,
                    1.236418134132634,
                    1.6754188547136784,
                    1.6416040100250626,
                    2.1614192903548224,
                    2.2227772227772227,
                    1.0744627686156922,
                    1.3126640830103764,
                    2.5378172271533943,
                    3.3125,
                    3.511622094476381,
                    1.5632816408204102,
                    3.620474406991261,
                    2.9863801074597025,
                    1.2648716343143394,
                    1.1611936571357222,
                    1.6129032258064515,
                    3.588845817181443,
                    1.3243378310844578,
                    2.5072082236429734,
                    2.395209580838323,
                    6.315657828914457,
                    3.0976767424431677,
                    2.8646157678415745,
                    3.5776832624468353,
                    2.1532298447671505,
                    2.465890599574415,
                    2.3985009369144286,
                    4.2473454091193,
                    3.889931207004378,
                    2.4518388791593697,
                    3.0016191306513886,
                    2.814610958218664,
                    3.019247704113725,
                    2.7127924340467895,
                    2.9375,
                    2.4390243902439024,
                    3.1791547188629847,
                    2.01325497061398,
                    3.1568356181612374,
                    2.774667164364813,
                    3.9639864949356007,
                    1.8900988859682062,
                    3.0257751214045574,
                    1.4541807697129248,
                    4.20042378162782,
                    6.213981458281133,
                    5.3235257449195865,
                    2.3436520867276602,
                    3.890759446315002,
                    3.973426924041113,
                    1.670015067805123,
                    4.529053129277093,
                    2.0648229257915154,
                    4.3825696091896615,
                    3.890759446315002,
                    8.32288794184006,
                    16.313295086056375,
                    3.9313885063227745,
                    3.1746031746031744,
                    4.294863744819792,
                    4.854730568661535,
                    4.84901641398321,
                    3.4904013961605584,
                    4.512409125094009,
                    3.8639489808678253,
                    3.8600874453466583,
                    3.6819770344483276,
                    3.9308963445167753,
                    2.601951463597698,
                    4.79820067474697,
                    4.549431321084865,
                    4.902083073468878,
                    5.147891755821271,
                    3.2209924138788706,
                    3.2060878243512976,
                    4.336957880264967,
                    3.152758663830852,
                    2.704056084126189,
                    3.0117470632341914,
                    4.2705072010018785,
                    4.996252810392206,
                    3.784661503872096,
                    2.6743314171457135,
                    4.125,
                    3.463365841460365,
                    4.259837601499063,
                    2.6200873362445414,
                    2.7948364456698833,
                    3.1179845347967072,
                    3.411086029596188,
                    3.2104934415990005,
                    3.59685275384039,
                    4.465510789572159,
                    2.5952858575727182,
                    4.499437570303712,
                    3.248375812093953,
                    2.4878109763720464,
                    3.965365792445727,
                    2.7535509593820087,
                    3.1855090568394755,
                    1.4865708931917552,
                    2.5209035317608888,
                    1.8934169278996866,
                    1.949025487256372,
                    2.875359419927491,
                    1.6360684401148995,
                    2.5387693846923463,
                    2.20247778751095,
                    2.0867174809446456,
                    1.440561192534135,
                    1.1214953271028036,
                    1.002004008016032,
                    1.1479910157224857,
                    0.9778112072207596,
                    1.0719182350741618,
                    1.7133566783391696,
                    0.924191332583989,
                    2.3267450587940957,
                    1.7747781527309086,
                    1.5994002249156567,
                    1.0393188079138493,
                    1.621351958094288,
                    1.518955561134823,
                    0.9715994020926756,
                    1.1489946296990134,
                    0.901352028042063,
                    2.127659574468085,
                    1.2106839740389417,
                    1.0360753963300462,
                    1.8509254627313656,
                    1.4882441220610305,
                    1.0870923403723605,
                    1.4262479669710997,
                    5.1077613055936215,
                    4.774931609052475,
                    5.52555569508979,
                    5.446161515453639,
                    6.3678361842926705,
                    4.3364158960259935,
                    1.1625,
                    1.825912956478239,
                    1.2125,
                    1.1605903872839662,
                    0.9479855307471623,
                    0.9656383245548031,
                    1.1587341141290806,
                    2.0210896309314585,
                    2.302140368342459,
                    1.0884523958463657,
                    0.9534562790114164,
                    0.9715994020926756,
                    1.098901098901099,
                    1.9784623090408215,
                    1.073389915127309,
                    1.56289072268067,
                    1.4886164623467601,
                    1.3994751968011996,
                    1.7609591607343575,
                    0.9498812648418947,
                    1.7649267743146826,
                    1.136221750530653,
                    0.9379689844922461,
                    2.1111805121798874,
                    2.2252781597699713,
                    2.1630407601900474,
                    1.115987460815047,
                    2.0125,
                    1.5451713395638629,
                    1.5625,
                    1.2642383277005884,
                    0.9364464976900987,
                    1.3744845682868925,
                    1.09104589917231,
                    0.8164275111331024,
                    1.0117411941044216,
                    1.4132066033016508,
                    0.8380237648530331,
                    0.7878939469734867,
                    1.075,
                    1.0613060307154452,
                    1.2125,
                    1.50018752344043,
                    1.4501812726590824,
                    1.0494752623688155,
                    1.1255627813906954,
                    0.7126781695423856,
                    1.1118051217988758,
                    1.0768845479589282,
                    0.9616585487698264,
                    0.8116883116883117,
                    1.5501937742217777,
                    1.1011011011011012,
                    0.9752438109527382,
                    0.9737827715355806,
                    1.0623672040994876,
                    1.7517517517517518,
                    1.5988008993255058,
                    2.4091826437941473,
                    1.1982026959560659,
                    1.2507817385866167,
                    0.974512743628186,
                    1.0889973713856553,
                    1.23688155922039,
                    1.735330836454432,
                    1.1008256192144108,
                    1.1502875718929733,
                    1.049082053203447,
                    1.040230605339015,
                    1.8573921715282973,
                    1.2879829936226084,
                    1.9262038774233896,
                    2.7843675864652266,
                    0.9283653243005896,
                    1.7957351290684624,
                    1.2492192379762648,
                    0.9248843894513186,
                    1.075268817204301,
                    1.3897583573306622,
                    0.9732967307212378,
                    1.1255627813906954,
                    1.2274549098196392,
                    0.9489324509926332,
                    1.2402906539714358,
                    1.2084215771770275,
                    0.9496438835436711,
                    1.0755377688844423,
                    1.236572570572071,
                    0.8443009684628756,
                    1.002004008016032,
                    1.047250966213689,
                    1.1,
                    0.9746345120579782,
                    1.5157209069272204,
                    1.0480349344978166,
                    5.388173521690211,
                    3.325774754346183,
                    2.424090965887792,
                    1.9879969992498125,
                    4.252400548696845,
                    5.122745490981964,
                    8.461827754795035,
                    9.561852452877293,
                    5.677564262540554,
                    6.4795087103647075,
                    11.7683763883689,
                    11.491809428535701,
                    11.861655637407916,
                    8.579289644822412,
                    3.53661584603849,
                    2.1997250343707035,
                    2.7125455230440787,
                    2.0734449163127655,
                    2.398201348988259,
                    2.8830620106872127,
                    2.2080040145527535,
                    2.3607294529103173,
                    4.101025256314078,
                    2.214160620465349,
                    2.205057929487978,
                    2.719298245614035,
                    4.217396761641773,
                    1.9920318725099602,
                    1.8165982331715815,
                    1.5831134564643798,
                    2.156739811912226,
                    2.106706556968337,
                    2.346480279580629,
                    4.129645851583031,
                    2.575,
                    2.0994751312171958,
                    2.6368407898025494,
                    2.261651880544796,
                    2.0940438871473352,
                    5.609573672400898,
                    3.127354935945742,
                    2.3625963690624223,
                    2.954431647471207,
                    2.8963795255930087,
                    2.0277882087870824,
                    1.6936488169364883,
                    2.3749685850716262,
                    2.330508474576271,
                    1.9772243774246028,
                    2.507522567703109,
                    2.4533001245330013,
                    3.6293339985033675,
                    4.437202306342441,
                    3.948519305260527,
                    2.93823455863966,
                    2.6739972510308636,
                    3.373734849431463,
                    3.779724655819775,
                    2.9798422436459244,
                    4.555097965805566,
                    3.9132070738743256,
                    2.130841121495327,
                    1.424287856071964,
                    1.78682993877296,
                    2.439634680345302,
                    3.381763527054108,
                    2.4922118380062304,
                    2.8814833375093962,
                    1.2358007739358383,
                    1.1126390798849857,
                    1.2484394506866416,
                    2.5378172271533943,
                    4.085967762089217,
                    10.723659542557181,
                    6.651662915728933,
                    4.898163188804198,
                    4.215134459036898,
                    4.111472131967008,
                    5.3895210704014005,
                    5.248031988004498,
                    6.167896909796071,
                    7.418508804795803,
                    7.763819095477387,
                    3.333747978604304,
                    7.185703574106474,
                    4.7011752938234554,
                    5.547919530176184,
                    5.1016589746788075,
                    6.253916530893596,
                    3.986503374156461,
                    4.254522769806613,
                    4.059133049361062,
                    4.02650994122796,
                    3.139795664091702,
                    1.455092824887105,
                    1.2118940529735132,
                    1.6364772017489069,
                    2.31278909863733,
                    2.627956451007383,
                    3.7217434744598474,
                    4.866483653606189,
                    4.683195592286501,
                    13.934016495876032,
                    4.038509627406852,
                    9.072706795144537,
                    8.952618453865338,
                    16.64580725907384,
                    4.65,
                    3.150393799224903,
                    3.7643821910955477,
                    3.409090909090909,
                    3.9929903617474025,
                    2.1972534332084894,
                    2.4621922259717537,
                    1.8506940102538452,
                    2.1284587454613746,
                    1.2983770287141074,
                    1.3537227375282026,
                    2.2429906542056073,
                    2.6503312914114265,
                    1.3141426783479349,
                    2.1483887084686484,
                    1.4536340852130325,
                    1.545941902505922,
                    1.3640345388562132,
                    1.6235793680529538,
                    2.0119970007498127,
                    3.310430980637102,
                    1.1252813203300824,
                    2.612080874042446,
                    1.7170586039567002,
                    2.8778778778778777,
                    2.7753469183647956,
                    2.3363318340829586,
                    2.0606968902210565,
                    1.0505252626313157,
                    1.261553834624032,
                    2.238339377266475,
                    3.426284856821308,
                    2.3505876469117277,
                    1.5746063484128967,
                    1.791755419120411,
                    2.4940765681506423,
                    1.3616489693941287,
                    1.988991743807856,
                    1.1641006383777694,
                    0.8989886377824947,
                    1.5130674002751032,
                    1.4498187726534184,
                    1.5720524017467248,
                    1.1009633429250594,
                    1.9129782445611403,
                    3.3629203650456305,
                    3.2383095773943484,
                    2.1125,
                    1.0744627686156922,
                    1.5873015873015872,
                    1.3391739674593242,
                    0.8868348738446166,
                    3.15,
                    3.1996000499937507,
                    5.376344086021505,
                    3.092269326683292,
                    2.291510142749812,
                    2.230576441102757,
                    3.7752305008721656,
                    6.034051076614922,
                    5.11988011988012,
                    4.2390896586219835,
                    4.489183443791422,
                    4.744616925388082,
                    1.921876949956321,
                    1.9879969992498125,
                    1.749562609347663,
                    2.1540388227927365,
                    3.834624031976018,
                    5.069297040829067,
                    4.37172120909318,
                    2.2439513601604615,
                    3.181137724550898,
                    3.399150212446888,
                    4.019975031210986,
                    2.077337004129646,
                    1.5992003998000999,
                    4.775,
                    3.3516758379189593,
                    3.313328332083021,
                    4.101732951003616,
                    4.184414933600602,
                    9.849812265331664,
                    4.359775140537164,
                    2.3238380809595203,
                    3.4767383691845923,
                    4.55743879472693,
                    4.677780542423489,
                    4.172932330827067,
                    2.8550056102730332,
                    2.5253156644580574,
                    3.8061850507073993,
                    3.7129641205150645,
                    1.550581468050519,
                    2.28236467947119,
                    3.4,
                    3.651825912956478,
                    2.224443889027743,
                    3.479236812570146,
                    3.221358736525445,
                    1.0622344413896525,
                    2.52784382430234,
                    1.798426376920195,
                    0.825,
                    1.0381488430268917,
                    2.3755938984746185,
                    3.655189620758483,
                    1.863431715857929,
                    1.3386713374202428,
                    3.2866783304173954,
                    2.8317253477007895,
                    2.143035135808622,
                    2.1627703462932866,
                    3.3654447641686476,
                    0.8876109513689211,
                    1.4128532133033258,
                    2.6993251687078232,
                    3.1222680154864495,
                    2.8489316506310134,
                    2.1638524077548467,
                    2.325,
                    2.937132858392701,
                    3.717611716109651,
                    2.919525888958203,
                    1.7129282320580146,
                    3.9453907815631264,
                    3.1694534564512105,
                    2.7965889139704037,
                    2.0622422197225347,
                    2.460347196203322,
                    3.121878121878122,
                    3.1464602322387316,
                    1.8667000751691305,
                    2.2072577628133185,
                    2.774653168353956,
                    2.638489433537577,
                    1.5248093988251468,
                    3.313328332083021,
                    3.123828564288392,
                    3.224193951512122,
                    3.425,
                    2.4875,
                    3.3012379642365888,
                    2.9790962573538615,
                    2.6351942050705635,
                    2.9742564358910273,
                    3.2495938007749032,
                    2.9279279279279278,
                    2.6838097615778307,
                    2.488122030507627,
                    2.074222166687492,
                    2.8399849868635054,
                    5.220432121893343,
                    4.981226533166458,
                    3.946546771574872,
                    2.8646484863647736,
                    2.9839518555667,
                    2.4523839163450765,
                    3.2626427406199023,
                    3.9775561097256857,
                    5.620082427875609,
                    2.0119970007498127,
                    4.761904761904762,
                    4.061992250968629,
                    3.675918979744936,
                    2.6490066225165565,
                    3.500437554694337,
                    3.7911122269645996,
                    3.5242839352428392,
                    2.784048156508653,
                    4.30937850292689,
                    2.401500938086304,
                    1.875,
                    2.008284172210368,
                    1.7037681880363138,
                    2.9146860145108833,
                    1.8625,
                    1.461769115442279,
                    1.3758599124452784,
                    1.3920240782543265,
                    0.7597459210362436,
                    1.3628407101775444,
                    1.5246188452886777,
                    2.274431392151962,
                    2.331380127166189,
                    1.5809284818067755,
                    2.1234074444166873,
                    2.614133833646029,
                    1.650825412706353,
                    1.536155863619333,
                    1.5740162398500936,
                    1.5138245965219568,
                    1.2112887112887112,
                    2.567635270541082,
                    1.5730337078651686,
                    2.0635317658829413,
                    1.2510947078693857,
                    1.3729405891163255,
                    0.7745159275452842,
                    1.3050570962479608,
                    1.6879219804951238,
                    2.105132037867464,
                    2.65,
                    2.770812437311936,
                    3.339147769748318,
                    1.9257221458046767,
                    1.3269904857285928,
                    1.3850761167956076,
                    1.2121969507623094,
                    1.026026026026026,
                    2.495944090852365,
                    2.9761160435163188,
                    1.125703564727955,
                    1.3120079970011245,
                    1.2152342771235278,
                    1.4722395508421708,
                    1.5242378810594703,
                    1.693002257336343,
                    1.058926124330385,
                    0.900225056264066,
                    0.9623797025371829,
                    1.026539809714572,
                    1.3358302122347065,
                    1.9460138104205902,
                    1.5978030208463363,
                    1.0223164193990775,
                    1.0126265783222903,
                    1.0501312664083011,
                    4.408091908091908,
                    5.026885081905714,
                    2.551594746716698,
                    2.443280977312391,
                    2.7433295753476137,
                    1.525,
                    2.325,
                    2.9287138584247256,
                    2.7342280195660353,
                    1.4996250937265683,
                    1.6122984626921635,
                    3.3291770573566084,
                    7.73286467486819,
                    1.2983770287141074,
                    1.0755377688844423,
                    1.4244658253155067,
                    0.7869098176367724,
                    1.0257693269952464,
                    0.6994753934549088,
                    0.8875,
                    1.025384519194698,
                    1.7745563609097725,
                    2.456448176463216,
                    2.655860349127182,
                    0.7375921990248782,
                    0.8386531480786081,
                    0.998377636340946,
                    0.7999000124984377,
                    2.186406796601699,
                    3.5146966854283925,
                    2.0372453443319585,
                    1.9259629814907453,
                    2.4359775140537163,
                    0.7634543178973717,
                    0.7987021090727567,
                    0.9007881896659578,
                    0.7125,
                    1.3123359580052494,
                    1.262342207224097,
                    2.40180135101326,
                    0.877742946708464,
                    0.9596211365902293,
                    0.9365634365634365,
                    1.2248468941382327,
                    0.9508319779807332,
                    0.7767476822851416,
                    0.7220216606498195,
                    0.9540547326136078,
                    0.9349289454001496,
                    0.7508447002878238,
                    1.11236095488064,
                    0.6118117118241978,
                    0.8258258258258259,
                    1.614922383575363,
                    1.9815553339980059,
                    1.9136960600375235,
                    1.6112915313514864,
                    0.6762680025046963,
                    0.7628814407203601,
                    0.9367974019485386,
                    1.3368315842078962,
                    1.8978649019852665,
                    1.2274549098196392,
                    1.1858694295343901,
                    3.311672081979505,
                    3.7783060177655448,
                    3.3462354850792857,
                    2.6378297287160897,
                    2.412198475190601,
                    1.927891837756635,
                    2.1510755377688846,
                    1.0998625171853518,
                    1.2613962782565256,
                    1.1864618458848508,
                    1.3013013013013013,
                    1.2862137862137861,
                    5.7117860267466565,
                    3.140640640640641,
                    4.909431605246721,
                    5.711072231942015,
                    4.078568747654198,
                    1.8245438640339915,
                    1.387326584176978,
                    1.1998500187476566,
                    0.9640666082383874,
                    1.0728542914171657,
                    1.2625,
                    0.914099674430253,
                    0.8983156581409857,
                    1.0635635635635636,
                    1.2740444666500126,
                    1.5531062124248498,
                    2.3678276121272863,
                    2.7514940239043826,
                    0.8877219304826206,
                    1.0391886816076124,
                    0.8237643534697953,
                    1.4376797099637455,
                    1.799550112471882,
                    0.9272021049993735,
                    1.0340102155226112,
                    1.0256410256410255,
                    0.9248843894513186,
                    1.425891181988743,
                    1.1120829688866676,
                    1.0384086075315901,
                    1.1134742900037533,
                    1.4223331253898939,
                    1.1009633429250594,
                    1.2612387612387612,
                    1.537307836520435,
                    1.809272521673577,
                    15.410406059853472,
                    12.409668577124346,
                    5.008117896840265,
                    5.541624874623872,
                    6.655056599079487,
                    2.993861956657898,
                    1.4595808383233533,
                    1.1397795591182365,
                    0.8129064532266133,
                    0.9484587545238987,
                    1.261553834624032,
                    1.137215696075981,
                    2.5827482447342027,
                    1.244031163608947,
                    1.3625,
                    1.4621344663834042,
                    1.8309505894156006,
                    1.0860067407314942,
                    5.251340900586254,
                    2.262217222847144,
                    2.0127515939492437,
                    2.899637545306837,
                    2.126063031515758,
                    1.4869423966012745,
                    1.5121219695076231,
                    1.6497937757780277,
                    0.9754877438719359,
                    1.6993627389728851,
                    1.687289088863892,
                    1.50018752344043,
                    1.537307836520435,
                ],
            },
        ];
        for test in tests {
            let mut dst = vec![];
            let src = test.input;

            super::encode(&src, &mut dst).expect("failed to encode");

            let mut got = vec![];
            super::decode(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, src, "{}", test.name);
        }
    }

    #[test]
    fn decode_influxdb() {
        // A block compressed with InfluxDB's gorilla encoder containing 507 0
        // values.
        let enc_influxdb = [
            16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 255, 255, 224, 0, 0, 0, 0, 0, 4,
        ];

        let mut got = vec![];
        let exp = vec![0.0; 507];
        super::decode_influxdb(&enc_influxdb, &mut got).expect("failed to decode");
        assert_eq!(got, exp);
    }
}
