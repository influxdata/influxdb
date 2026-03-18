#![warn(missing_docs)]
//! A crate for creating and parsing [`TableBatch`]es
//!
//! [`TableBatch`]: generated_types::influxdata::pbdata::v1::TableBatch

/// Builders for producing arrays of values for the batch
pub mod builder;
/// Helper traits and function mainly for parsing [`Column`]s
///
/// [`Column`]: generated_types::influxdata::pbdata::v1::Column
pub mod column;
/// Helper traits and function mainly for parsing [`PackedStrings`] and [`InternedStrings`]
///
/// [`PackedStrings`]: generated_types::influxdata::pbdata::v1::PackedStrings
/// [`InternedStrings`]: generated_types::influxdata::pbdata::v1::InternedStrings
pub mod values;

/// This represent a collection of values, like [`InternedStrings`], [`PackedStrings`], or a simple
/// [`Vec`].
///
/// [`InternedStrings`]: generated_types::influxdata::pbdata::v1::InternedStrings
/// [`PackedStrings`]: generated_types::influxdata::pbdata::v1::PackedStrings
pub trait ValueCollection {
    /// The item inside this collection, allowing for internal lifetimes
    type Item<'a>
    where
        Self: 'a;

    /// Try to get the item at the given index
    fn get(&self, idx: usize) -> Option<Self::Item<'_>>;

    /// Get the number of items contained in this collection
    fn len(&self) -> usize;

    /// Check if there are no items in this collection
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
