use crate::column::cmp::Operator;
use std::{
    convert::TryFrom,
    fmt::{Debug, Display},
};

// A `Transcoder` describes behaviour to encode and decode from one scalar type
// to another.
//
// All scalar encodings within the Read Buffer require a `Transcoder`
// implementation to define how data should be encoded before they store it and
// how they should decode it before returning it to callers.
//
// `P` is a physical type that is stored directly within an encoding, `L` is
// a logical type callers expect to be returned.
pub trait Transcoder<P, L>: Debug + Display {
    /// A function that encodes a logical value into a physical representation.
    fn encode(&self, _: L) -> P;

    /// A function that attempts to encode a logical value, within the context
    /// of a comparison operator, into a physical representation.
    ///
    /// Implementation should return a suitable operator for the physical
    /// representation, which may differ from the provided operator.
    fn encode_comparable(&self, _: L, _: Operator) -> Option<(P, Operator)>;

    /// A function to decode a physical representation back into a logical value.
    fn decode(&self, _: P) -> L;
}

/// A No-op transcoder - useful when you can't do any useful transcoding, e.g.,
/// because you have a very high entropy 64-bit column.
///
/// `NoOpTranscoder` implements `Transcoder` in terms of `T` -> `T` and just
/// returns the provided argument.
#[derive(Debug)]
pub struct NoOpTranscoder {}
impl<T> Transcoder<T, T> for NoOpTranscoder {
    fn encode(&self, v: T) -> T {
        v
    }

    fn encode_comparable(&self, v: T, op: Operator) -> Option<(T, Operator)> {
        Some((v, op))
    }

    fn decode(&self, v: T) -> T {
        v
    }
}

impl Display for NoOpTranscoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "None")
    }
}

/// An encoding that will coerce scalar types from a logical type `L` to a
/// physical type `P`, and back again.
///
/// `ByteTrimmer` is only generic over types that implement `From` or `TryFrom`,
/// which does not cover float -> integer conversion.
///
/// #Panics
///
/// It is the caller's responsibility to ensure that conversions involving
/// `P::TryFrom(L)` will always succeed by, e.g., checking each value to be
/// transcoded.
#[derive(Debug)]
pub struct ByteTrimmer {}
impl<P, L> Transcoder<P, L> for ByteTrimmer
where
    L: From<P>,
    P: TryFrom<L> + PartialEq + PartialOrd,
    <P as TryFrom<L>>::Error: std::fmt::Debug,
{
    fn encode(&self, v: L) -> P {
        P::try_from(v).unwrap()
    }

    fn encode_comparable(&self, v: L, op: Operator) -> Option<(P, Operator)> {
        P::try_from(v).ok().map(|p| (p, op))
    }

    fn decode(&self, v: P) -> L {
        L::from(v)
    }
}

impl Display for ByteTrimmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BT")
    }
}

/// An encoding that forcefully converts logical `f64` values into other integer
/// types. It is the caller's responsibility to ensure that conversion can take
/// place without loss of precision.
#[derive(Debug)]
pub struct FloatByteTrimmer {}
macro_rules! make_float_trimmer {
    ($type:ty) => {
        #[allow(clippy::float_cmp)]
        impl Transcoder<$type, f64> for FloatByteTrimmer {
            fn encode(&self, v: f64) -> $type {
                // shouldn't be too expensive as only called during column
                // creation and when passing in single literals for
                // predicate evaluation.
                assert!(v == (v as $type) as f64);
                v as $type
            }

            fn encode_comparable(&self, v: f64, op: Operator) -> Option<($type, Operator)> {
                assert!(v <= <$type>::MAX as f64);
                if v == ((v as $type) as f64) {
                    return Some((v as $type, op));
                }

                match op {
                    Operator::Equal => {
                        None // no encoded values will == v
                    }
                    Operator::NotEqual => {
                        None // all encoded values will != v
                    }
                    Operator::LT => {
                        // convert to next highest encodable value. For example
                        // given '< 23.2` return 24.0 encoded as the physical
                        // type. < 23.2 is logically equivalent to < 24.0 since
                        // there are no valid values in the domain (23.2, 24.0).
                        Some((v.ceil() as $type, op))
                    }
                    Operator::LTE => {
                        // convert to next highest encodable value and change
                        // operator to <.
                        // For example given '<= 23.2` return 24.0 encoded as
                        // the physical type. <= 23.2 is logically equivalent
                        // to < 24.0 since there are no valid values in the
                        // domain [23.2, 24.0).
                        Some((v.ceil() as $type, Operator::LT))
                    }
                    Operator::GT => {
                        // convert to next lowest encodable value. For example
                        // given '> 23.2` return 23.0 encoded as the physical
                        // type. > 23.2 is logically equivalent to > 23.0 since
                        // there are no valid values in the domain (23.0, 23.2].
                        Some((v.floor() as $type, op))
                    }
                    Operator::GTE => {
                        // convert to next lowest encodable value and change
                        // operator to >.
                        // For example given '>= 23.2` return 23.0 encoded as
                        // the physical type. >= 23.2 is logically equivalent
                        // to > 24.0 since there are no valid values in the
                        // domain [23.2, 24.0).
                        Some((v.floor() as $type, Operator::GT))
                    }
                }
            }

            fn decode(&self, v: $type) -> f64 {
                v.into()
            }
        }
    };
}

make_float_trimmer!(u8);
make_float_trimmer!(i8);
make_float_trimmer!(u16);
make_float_trimmer!(i16);
make_float_trimmer!(u32);
make_float_trimmer!(i32);

impl Display for FloatByteTrimmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FBT")
    }
}

//
// TODO(edd): soon to be adding the following
//
//  * FrameOfReferenceTranscoder: a transcoder that will apply a transformation
//        to logical values and then optionally apply a byte trimming to the
//        result.

#[cfg(test)]
use std::{sync::atomic, sync::atomic::AtomicUsize, sync::Arc};
#[cfg(test)]
/// A mock implementation of Transcoder that tracks calls to encode and decode.
/// This is useful for testing encoder implementations.
#[derive(Debug)]
pub struct MockTranscoder {
    encoding_calls: AtomicUsize,
    decoding_calls: AtomicUsize,
    partial_cmp_calls: AtomicUsize,
}

#[cfg(test)]
impl Default for MockTranscoder {
    fn default() -> Self {
        Self {
            encoding_calls: AtomicUsize::default(),
            decoding_calls: AtomicUsize::default(),
            partial_cmp_calls: AtomicUsize::default(),
        }
    }
}

#[cfg(test)]
impl MockTranscoder {
    pub fn encodings(&self) -> usize {
        self.encoding_calls.load(atomic::Ordering::Relaxed)
    }

    pub fn decodings(&self) -> usize {
        self.decoding_calls.load(atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
impl<T> Transcoder<T, T> for MockTranscoder {
    fn encode(&self, v: T) -> T {
        self.encoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        v
    }

    fn encode_comparable(&self, v: T, op: Operator) -> Option<(T, Operator)> {
        self.encoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        Some((v, op))
    }

    fn decode(&self, v: T) -> T {
        self.decoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        v
    }
}

#[cfg(test)]
impl<T> Transcoder<T, T> for Arc<MockTranscoder> {
    fn encode(&self, v: T) -> T {
        self.encoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        v
    }

    fn encode_comparable(&self, v: T, op: Operator) -> Option<(T, Operator)> {
        self.encoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        Some((v, op))
    }

    fn decode(&self, v: T) -> T {
        self.decoding_calls.fetch_add(1, atomic::Ordering::Relaxed);
        v
    }
}

#[cfg(test)]
impl Display for MockTranscoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}
