use std::sync::Arc;

use http::{Error, Uri};

/// Types that can be converted to a [Uri] returning an [Error] on
/// failure.
pub trait TryIntoUri {
    fn try_into_uri(&self) -> Result<Uri, Error>;
}

impl TryIntoUri for str {
    #[inline]
    fn try_into_uri(&self) -> Result<Uri, Error> {
        self.try_into().map_err(Error::from)
    }
}

impl TryIntoUri for String {
    #[inline]
    fn try_into_uri(&self) -> Result<Uri, Error> {
        self.try_into().map_err(Error::from)
    }
}

impl TryIntoUri for Uri {
    #[inline]
    fn try_into_uri(&self) -> Result<Uri, Error> {
        Ok(self.clone())
    }
}

impl<T: TryIntoUri + ?Sized> TryIntoUri for Arc<T> {
    #[inline]
    fn try_into_uri(&self) -> Result<Uri, Error> {
        self.as_ref().try_into_uri()
    }
}
