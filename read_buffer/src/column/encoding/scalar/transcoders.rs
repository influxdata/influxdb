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
    fn encode(&self, _: L) -> P;
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
    P: TryFrom<L>,
    <P as TryFrom<L>>::Error: std::fmt::Debug,
{
    fn encode(&self, v: L) -> P {
        P::try_from(v).unwrap()
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

//
// TODO(edd): soon to be adding the following
//
//  * FloatByteTrimmer: a transcoder that will coerce `f64` values into signed
//        and unsigned integers.
//
//  * FrameOfReferenceTranscoder: a transcoder that will apply a transformation
//        to logical values and then optionally apply a byte trimming to the
//        result.

#[cfg(test)]
use std::{sync::atomic::AtomicUsize, sync::atomic::Ordering, sync::Arc};
#[cfg(test)]
/// A mock implementation of Transcoder that tracks calls to encode and decode.
/// This is useful for testing encoder implementations.
#[derive(Debug)]
pub struct MockTranscoder {
    encoding_calls: AtomicUsize,
    decoding_calls: AtomicUsize,
}

#[cfg(test)]
impl Default for MockTranscoder {
    fn default() -> Self {
        Self {
            encoding_calls: AtomicUsize::default(),
            decoding_calls: AtomicUsize::default(),
        }
    }
}

#[cfg(test)]
impl MockTranscoder {
    pub fn encodings(&self) -> usize {
        self.encoding_calls.load(Ordering::Relaxed)
    }

    pub fn decodings(&self) -> usize {
        self.decoding_calls.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
impl<T> Transcoder<T, T> for MockTranscoder {
    fn encode(&self, v: T) -> T {
        self.encoding_calls.fetch_add(1, Ordering::Relaxed);
        v
    }

    fn decode(&self, v: T) -> T {
        self.decoding_calls.fetch_add(1, Ordering::Relaxed);
        v
    }
}

#[cfg(test)]
impl<T> Transcoder<T, T> for Arc<MockTranscoder> {
    fn encode(&self, v: T) -> T {
        self.encoding_calls.fetch_add(1, Ordering::Relaxed);
        v
    }

    fn decode(&self, v: T) -> T {
        self.decoding_calls.fetch_add(1, Ordering::Relaxed);
        v
    }
}

#[cfg(test)]
impl Display for MockTranscoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}
