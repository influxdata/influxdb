use std::fmt::Display;

use crate::query_adaptor::QueryAdaptor;

/// An opaque generational identifier of a buffer in a [`PartitionData`].
///
/// [`PartitionData`]: super::PartitionData
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct BatchIdent(u64);

impl BatchIdent {
    /// Return the next unique value.
    pub(super) fn next(&mut self) -> Self {
        self.0 += 1;
        Self(self.0)
    }

    /// Only for tests, this allows reading the opaque identifier to assert the
    /// value changing between persist ops.
    #[cfg(test)]
    pub(super) fn get(&self) -> u64 {
        self.0
    }
}

impl Display for BatchIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A type wrapper over [`QueryAdaptor`] that statically ensures only batches of
/// data from [`PartitionData::mark_persisting()`] are given to
/// [`PartitionData::mark_persisted()`].
///
/// Cloning this type is relatively cheap.
///
/// [`PartitionData::mark_persisting()`]: super::PartitionData::mark_persisting
/// [`PartitionData::mark_persisted()`]: super::PartitionData::mark_persisted
#[derive(Debug, Clone)]
pub struct PersistingData {
    data: QueryAdaptor,
    batch_ident: BatchIdent,
}

impl PersistingData {
    pub(super) fn new(data: QueryAdaptor, batch_ident: BatchIdent) -> Self {
        Self { data, batch_ident }
    }

    pub(super) fn batch_ident(&self) -> BatchIdent {
        self.batch_ident
    }

    pub(crate) fn query_adaptor(&self) -> QueryAdaptor {
        self.data.clone()
    }
}

impl std::ops::Deref for PersistingData {
    type Target = QueryAdaptor;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_ident() {
        let mut b = BatchIdent::default();

        assert_eq!(b.get(), 0);

        assert_eq!(b.next().get(), 1);
        assert_eq!(b.get(), 1);

        assert_eq!(b.next().get(), 2);
        assert_eq!(b.get(), 2);
    }
}
