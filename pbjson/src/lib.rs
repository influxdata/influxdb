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
    use std::str::FromStr;
    /// Used to parse a number from either a string or its raw representation
    #[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Hash, Ord, Eq)]
    pub struct NumberDeserialize<T>(pub T);

    #[derive(serde::Deserialize)]
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
        #[allow(deprecated)]
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
}
