use std::convert::TryFrom;

// A `Transcoders` describes behaviour to encode and decode from one scalar type
// to another.
//
// All scalar encodings within the Read Buffer require a `Transcoder`
// implementation to define how data should be encoded before they store it and
// how they should decode it before returning it to callers.
//
// `P` is a physical type that is stored directly within an encoding, `L` is
// a logical type callers expect to be returned.
pub trait Transcoder<P, L> {
    fn encode(&self, _: L) -> P;
    fn decode(&self, _: P) -> L;
}

/// A No-op transcoder
pub struct NoOpTranscoder {}
impl<T> Transcoder<T, T> for NoOpTranscoder {
    fn encode(&self, v: T) -> T {
        v
    }

    fn decode(&self, v: T) -> T {
        v
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
/// `P::TryFrom(L)` will always succeed.
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

//
// TODO(edd): shortly to be adding the following
//
//  * FloatByteTrimmer: a transcoder that will coerce `f64` values into signed
//        and unsigned integers.
//
//  * FrameOfReferenceTranscoder: a transcoder that will apply a transformation
//        to logical values and then optionally apply a byte trimming to the
//        result.
