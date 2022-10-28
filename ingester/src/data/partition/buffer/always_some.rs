//! A helper type that ensures an `Option` is always `Some` once the guard is
//! dropped.

/// A helper type that aims to ease calling methods on a type that takes `self`,
/// that must always be restored at the end of the method call.
#[derive(Debug)]
pub(super) struct AlwaysSome<T>(Option<T>);

impl<T> Default for AlwaysSome<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> std::ops::Deref for AlwaysSome<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl<T> AlwaysSome<T> {
    /// Wrap `value` in an [`AlwaysSome`].
    pub(super) fn new(value: T) -> Self {
        Self(Some(value))
    }

    pub(super) fn mutate<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(T) -> (T, R),
    {
        let value = std::mem::take(&mut self.0);
        let (value, ret) = f(value.expect("AlwaysSome value is None!"));
        self.0 = Some(value);
        ret
    }

    /// Deconstruct `self`, returning the inner value.
    pub(crate) fn into_inner(self) -> T {
        self.0.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_some() {
        let mut a = AlwaysSome::<usize>::default();

        let ret = a.mutate(|value| {
            assert_eq!(value, 0);
            (42, true)
        });
        assert!(ret);

        let ret = a.mutate(|value| {
            assert_eq!(value, 42);
            (13, "bananas")
        });
        assert_eq!(ret, "bananas");

        assert_eq!(a.into_inner(), 13);
    }
}
