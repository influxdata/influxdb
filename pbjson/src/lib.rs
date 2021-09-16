#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

#[doc(hidden)]
pub mod private {
    /// Re-export base64
    pub use base64;

    use serde::Deserialize;
    use std::str::FromStr;

    /// Used to parse a number from either a string or its raw representation
    #[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Hash, Ord, Eq)]
    pub struct NumberDeserialize<T>(pub T);

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Content<'a, T> {
        Str(&'a str),
        Number(T),
    }

    impl<'de, T> serde::Deserialize<'de> for NumberDeserialize<T>
    where
        T: FromStr + serde::Deserialize<'de>,
        <T as FromStr>::Err: std::error::Error,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let content = Content::deserialize(deserializer)?;
            Ok(Self(match content {
                Content::Str(v) => v.parse().map_err(serde::de::Error::custom)?,
                Content::Number(v) => v,
            }))
        }
    }

    #[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Hash, Ord, Eq)]
    pub struct BytesDeserialize<T>(pub T);

    impl<'de, T> Deserialize<'de> for BytesDeserialize<T>
    where
        T: From<Vec<u8>>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let s: &str = Deserialize::deserialize(deserializer)?;
            let decoded = base64::decode(s).map_err(serde::de::Error::custom)?;
            Ok(Self(decoded.into()))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use bytes::Bytes;
        use serde::de::value::{BorrowedStrDeserializer, Error};

        #[test]
        fn test_bytes() {
            let raw = vec![2, 5, 62, 2, 5, 7, 8, 43, 5, 8, 4, 23, 5, 7, 7, 3, 2, 5, 196];
            let encoded = base64::encode(&raw);

            let deserializer = BorrowedStrDeserializer::<'_, Error>::new(&encoded);
            let a: Bytes = BytesDeserialize::deserialize(deserializer).unwrap().0;
            let b: Vec<u8> = BytesDeserialize::deserialize(deserializer).unwrap().0;

            assert_eq!(raw.as_slice(), &a);
            assert_eq!(raw.as_slice(), &b);
        }
    }
}
