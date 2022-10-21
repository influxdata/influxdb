//! A helper type that ensures an `Option` is always `Some` once the guard is
//! dropped.

/// A guard through which a value can be placed back into the [`AlwaysSome`].
#[derive(Debug)]
#[must_use = "Guard must be used to restore the value"]
pub(super) struct Guard<'a, T>(&'a mut Option<T>);

impl<'a, T> Guard<'a, T> {
    /// Store `value` in the [`AlwaysSome`] for subsequent
    /// [`AlwaysSome::take()`] calls.
    pub(super) fn store(self, value: T) {
        assert!(self.0.is_none());
        *self.0 = Some(value);
    }
}

/// A helper type that aims to ease working with an [`Option`] that must always
/// be restored in a given scope.
///
/// Accessing the value within an [`AlwaysSome`] returns a [`Guard`], which MUST
/// be used to store the value before going out of scope. Failure to store a
/// value cause a subsequent [`Self::take()`] call to panic.
///
/// Failing to store a value in the [`Guard`] causes a compiler warning, however
/// this does not prevent failing to return a value to the [`AlwaysSome`] as the
/// warning can be falsely silenced by using it within one conditional code path
/// and not the other.
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

    /// Read the value.
    pub(super) fn take(&mut self) -> (Guard<'_, T>, T) {
        let value = std::mem::take(&mut self.0);

        (
            Guard(&mut self.0),
            value.expect("AlwaysSome value is None!"),
        )
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

        let (guard, value) = a.take();
        assert_eq!(value, 0);
        guard.store(42);

        let (guard, value) = a.take();
        assert_eq!(value, 42);
        guard.store(24);

        assert_eq!(a.into_inner(), 24);
    }

    #[test]
    #[should_panic = "AlwaysSome value is None!"]
    fn test_drops_guard() {
        let mut a = AlwaysSome::<usize>::default();
        {
            let _ = a.take();
        }
        let _ = a.take();
    }
}
