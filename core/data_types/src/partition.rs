//! Types having to do with partitions.

use crate::{NamespacePartitionTemplateOverride, PARTITION_KEY_DELIMITER};

use super::{ColumnsByName, SortKeyIds, TableId, Timestamp};

use schema::sort::SortKey;
use sha2::Digest;
use std::{fmt::Display, sync::Arc};
use thiserror::Error;

/// Just a [`TableBatch`] but without the table id.
///
/// [`TableBatch`]: generated_types::influxdata::pbdata::v1::TableBatch
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableBatchWithoutId {
    /// The columns, same as exist in `TableBatch`
    pub columns: Vec<generated_types::influxdata::pbdata::v1::Column>,
    /// The number of rows that should exist in each column, same as `TableBatch`
    pub row_count: u32,
}

/// Serialise a [`PartitionHashId`] to the `PartitionHashIdentifier` protobuf representation.
impl From<&PartitionHashId>
    for generated_types::influxdata::iox::catalog::v1::PartitionHashIdentifier
{
    fn from(value: &PartitionHashId) -> Self {
        Self {
            hash_id: value.as_bytes().to_owned(),
        }
    }
}

/// Unique ID for a `Partition`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, sqlx::FromRow)]
#[sqlx(transparent)]
pub struct PartitionId(i64);

#[expect(missing_docs)]
impl PartitionId {
    pub const fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Defines a partition via an arbitrary string within a table within
/// a namespace.
///
/// Implemented as a reference-counted string, serialisable to
/// the Postgres VARCHAR data type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey(Arc<str>);

impl PartitionKey {
    /// Returns true if this instance of [`PartitionKey`] is backed by the same
    /// string storage as other.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Returns underlying string.
    pub fn inner(&self) -> &str {
        &self.0
    }

    /// Returns the bytes of the inner string.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PartitionKey {
    fn from(s: String) -> Self {
        assert!(!s.is_empty());
        Self(s.into())
    }
}

impl From<&str> for PartitionKey {
    fn from(s: &str) -> Self {
        assert!(!s.is_empty());
        Self(s.into())
    }
}

impl sqlx::Type<sqlx::Postgres> for PartitionKey {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        // Store this type as VARCHAR
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR")
    }
}

impl sqlx::postgres::PgHasArrayType for PartitionKey {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::array_of("VARCHAR")
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for PartitionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::database::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode(&self.0, buf)
    }
}

impl sqlx::Decode<'_, sqlx::Postgres> for PartitionKey {
    fn decode(
        value: <sqlx::Postgres as sqlx::database::Database>::ValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?.into(),
        ))
    }
}

impl sqlx::Type<sqlx::Sqlite> for PartitionKey {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <String as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl sqlx::Encode<'_, sqlx::Sqlite> for PartitionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::database::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <String as sqlx::Encode<sqlx::Sqlite>>::encode(self.0.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::Sqlite> for PartitionKey {
    fn decode(
        value: <sqlx::Sqlite as sqlx::database::Database>::ValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <String as sqlx::Decode<sqlx::Sqlite>>::decode(value)?.into(),
        ))
    }
}

/// An error struct that could be returned from [`NamespacePartitionTemplateOverride::part_key()`]
/// if validation fails. [`Self::expected`] will be equal to the number of parts which the template had,
/// and [`Self::found`] will be equal to the number of fields passed in as the argument `parts`
#[derive(Debug, Copy, Clone, Error)]
#[error(
    "Expected {expected} parts in key due to template passed in, found {found}. (Note: this may be due to a provided `part` containing the delimiter, `|`, resulting in the key containing more parts than necessary)"
)]
pub struct MismatchedNumPartsError {
    /// The number of parts that the tmpl passed into [`NamespacePartitionTemplateOverride::part_key()`] had
    pub expected: usize,
    /// The number of parts that were passed into [`NamespacePartitionTemplateOverride::part_key()`]
    pub found: usize,
}

/// A utility struct to create a [`PartitionKey`] for a specific
/// [`NamespacePartitionTemplateOverride`] and verify that this key has the correct amount of
/// `parts` in it to be used with the provided template.
#[derive(Debug)]
pub struct PartitionKeyBuilder {
    /// The incremental string that is built as [`Self::push()`] is called multiple times, and what
    /// will be turned into the [`PartitionKey`] if validation succeeds.
    key: String,
    /// The number of expected `parts` that this key should have, based on the template provided in
    /// [`Self::new()`]
    expected_parts: usize,
}

impl PartitionKeyBuilder {
    /// Create a new [`PartitionKeyBuilder`] with a preallocated string that expects
    /// [`tmpl.parts()`](`NamespacePartitionTemplateOverride::parts()`)[`.count()`] calls to
    /// [`Self::push()`].
    ///
    /// [`.count()`]: ::std::iter::Iterator::count
    pub fn new(tmpl: &NamespacePartitionTemplateOverride) -> Self {
        let expected_parts = tmpl.parts().count();
        Self {
            key: String::with_capacity((expected_parts * 2) - 1),
            expected_parts,
        }
    }

    /// Push a `T` that implements [`std::fmt::Display`] to the internally-stored string. This
    /// method will automatically add the `|` to separate the `T` from the rest of the string with
    /// necessary, and returns `&mut Self` to facilitate chaining.
    pub fn push<T: Display>(mut self, part: T) -> Self {
        use std::fmt::Write as _;

        if self.key.is_empty() {
            write!(self.key, "{part}").unwrap();
        } else {
            write!(self.key, "{PARTITION_KEY_DELIMITER}{part}").unwrap();
        }
        self
    }

    /// Attempt to build [`self`], returning a [`MismatchedNumPartsError`] if validation fails.
    /// Validation fails only if the number of parts (which are delimited by a `|` character) in
    /// [`self.key`] does not equal [`self.expected_parts`] (which is equal to the number of parts
    /// in the template passed into [`Self::new()`]. If validation does not fail, [`self.key`] will
    /// be turned into a [`PartitionKey`] and returned.
    ///
    /// [`self`]: Self
    /// [`self.key`]: Self::key
    /// [`self.expected_parts`]: Self::expected_parts
    pub fn build(self) -> Result<PartitionKey, MismatchedNumPartsError> {
        let found = self.key.chars().filter(|c| *c == '|').count() + 1;

        (found == self.expected_parts)
            .then(|| self.key.into())
            .ok_or(MismatchedNumPartsError {
                found,
                expected: self.expected_parts,
            })
    }
}

const PARTITION_HASH_ID_SIZE_BYTES: usize = 32;

/// Uniquely identify a partition based on its table ID and partition key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, sqlx::FromRow)]
#[sqlx(transparent)]
pub struct PartitionHashId(Arc<[u8; PARTITION_HASH_ID_SIZE_BYTES]>);

impl std::fmt::Display for PartitionHashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &*self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Re-use the `Display` format as the `Debug` format rather than using the auto-derived `Debug`
/// implementation so that `PartitionHashId` values are printed out in a one-line hex string rather
/// than a list of 32 byte values with each byte on its own line. This makes test output, `dbg!`
/// output, etc easier to read.
impl std::fmt::Debug for PartitionHashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::hash::Hash for PartitionHashId {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // the slice is already hashed, so we can be a bit more efficient:
        // A hash of an object is technically only 64bits (this is what `Hasher::finish()` will produce). We assume that
        // the SHA256 hash sum that was used to create the partition hash is good enough so that every 64-bit slice of
        // it is a good hash candidate for the entire object. Hence, we only forward the first 64 bits to the hasher and
        // call it a day.

        // There is currently no nice way to slice fixed-sized arrays, see:
        // https://github.com/rust-lang/rust/issues/90091
        //
        // So we implement this the hard way (to avoid some nasty panic paths that are quite expensive within a hash function).
        // Conversion borrowed from https://github.com/rust-lang/rfcs/issues/1833#issuecomment-269509262
        const N_BYTES: usize = u64::BITS as usize / 8;

        const _: () = assert!(PARTITION_HASH_ID_SIZE_BYTES >= N_BYTES);
        let ptr = self.0.as_ptr() as *const [u8; N_BYTES];
        let sub: &[u8; N_BYTES] = unsafe { &*ptr };

        state.write_u64(u64::from_ne_bytes(*sub));
    }
}

/// Reasons bytes specified aren't a valid `PartitionHashId`.
#[derive(Debug, Error)]
pub enum PartitionHashIdError {
    /// The bytes specified were not valid
    #[error("Could not interpret bytes as `PartitionHashId`: {data:?}")]
    InvalidBytes {
        /// The bytes used in the attempt to create a `PartitionHashId`
        data: Vec<u8>,
    },
}

impl TryFrom<&[u8]> for PartitionHashId {
    type Error = PartitionHashIdError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        let data: [u8; PARTITION_HASH_ID_SIZE_BYTES] =
            data.try_into()
                .map_err(|_| PartitionHashIdError::InvalidBytes {
                    data: data.to_vec(),
                })?;

        Ok(Self(Arc::new(data)))
    }
}

impl PartitionHashId {
    /// Create a new `PartitionHashId`.
    pub fn new(table_id: TableId, partition_key: &PartitionKey) -> Self {
        Self::from_raw(table_id, partition_key.as_bytes())
    }

    /// Create a new `PartitionHashId`
    pub fn from_raw(table_id: TableId, key: &[u8]) -> Self {
        // The hash ID of a partition is the SHA-256 of the `TableId` then the `PartitionKey`. This
        // particular hash format was chosen so that there won't be collisions and this value can
        // be used to uniquely identify a Partition without needing to go to the catalog to get a
        // database-assigned ID. Given that users might set their `PartitionKey`, a cryptographic
        // hash scoped by the `TableId` is needed to prevent malicious users from constructing
        // collisions. This data will be held in memory across many services, so SHA-256 was chosen
        // over SHA-512 to get the needed attributes in the smallest amount of space.
        let mut inner = sha2::Sha256::new();

        let table_bytes = table_id.to_be_bytes();
        // Avoiding collisions depends on the table ID's bytes always being a fixed size. So even
        // though the current return type of `TableId::to_be_bytes` is `[u8; 8]`, we're asserting
        // on the length here to make sure this code's assumptions hold even if the type of
        // `TableId` changes in the future.
        assert_eq!(table_bytes.len(), 8);
        inner.update(table_bytes);

        inner.update(key);
        Self(Arc::new(inner.finalize().into()))
    }

    /// Read access to the bytes of the hash identifier.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Size in bytes including `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.len()
    }

    /// Create a new `PartitionHashId` for cases in tests where you need some value but the value
    /// doesn't matter. Public and not test-only so that other crates' tests can use this.
    pub fn arbitrary_for_testing() -> Self {
        Self::new(TableId::new(0), &PartitionKey::from("arbitrary"))
    }
}

impl sqlx::encode::Encode<'_, sqlx::Postgres> for PartitionHashId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        buf.extend_from_slice(self.0.as_ref());

        Ok(sqlx::encode::IsNull::No)
    }
}

impl<'q> sqlx::encode::Encode<'q, sqlx::Sqlite> for &'q PartitionHashId {
    fn encode_by_ref(
        &self,
        args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        args.push(sqlx::sqlite::SqliteArgumentValue::Blob(
            std::borrow::Cow::Borrowed(self.0.as_ref()),
        ));

        Ok(sqlx::encode::IsNull::No)
    }
}

impl<'r, DB: ::sqlx::Database> ::sqlx::decode::Decode<'r, DB> for PartitionHashId
where
    &'r [u8]: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: DB::ValueRef<'r>,
    ) -> ::std::result::Result<
        Self,
        ::std::boxed::Box<
            dyn ::std::error::Error + 'static + ::std::marker::Send + ::std::marker::Sync,
        >,
    > {
        let data = <&[u8] as ::sqlx::decode::Decode<'r, DB>>::decode(value)?;
        let data: [u8; PARTITION_HASH_ID_SIZE_BYTES] = data.try_into()?;
        Ok(Self(Arc::new(data)))
    }
}

impl<'r, DB: ::sqlx::Database> ::sqlx::Type<DB> for PartitionHashId
where
    &'r [u8]: ::sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <&[u8] as ::sqlx::Type<DB>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for PartitionHashId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <&[u8] as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

/// Data object for a partition. The combination of table and key are unique (i.e. only one record
/// can exist for each combo)
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow, Hash)]
pub struct Partition {
    /// the id of the partition
    pub id: PartitionId,
    /// The unique hash derived from the table ID and partition key.
    hash_id: PartitionHashId,
    /// the table the partition is under
    pub table_id: TableId,
    /// the string key of the partition
    pub partition_key: PartitionKey,

    /// Vector of column IDs that describes how *every* parquet file
    /// in this [`Partition`] is sorted. The sort key contains all the
    /// primary key (PK) columns that have been persisted, and nothing
    /// else. The PK columns are all `tag` columns and the `time`
    /// column.
    ///
    /// Even though it is possible for both the unpersisted data
    /// and/or multiple parquet files to contain different subsets of
    /// columns, the partition's sort key is guaranteed to be
    /// "compatible" across all files. Compatible means that the
    /// parquet file is sorted in the same order as the partition
    /// sort key after removing any missing columns.
    ///
    /// Partitions are initially created before any data is persisted
    /// with an empty sort key. The partition sort key is updated as
    /// needed when data is persisted to parquet files: both on the
    /// first persist when the sort key is empty, as on subsequent
    /// persist operations when new tags occur in newly inserted data.
    ///
    /// Updating inserts new columns into the existing sort key. The order
    /// of the existing columns relative to each other is NOT changed.
    ///
    /// For example, updating `A,B,C` to either `A,D,B,C` or `A,B,C,D`
    /// is legal. However, updating to `A,C,D,B` is not because the
    /// relative order of B and C has been reversed.
    sort_key_ids: SortKeyIds,

    /// The time at which the newest file of the partition is created
    pub new_file_at: Option<Timestamp>,

    /// The time at which the partition was last cold compacted
    pub cold_compact_at: Option<Timestamp>,

    /// The time at which this partition was created, or `None` if this partition was created before
    /// this field existed.
    created_at: Option<Timestamp>,

    /// The maximum time of data that can exist in this partition based on the partition template.
    /// This is calculated when the partition is created and used for efficient retention queries.
    pub max_time: Option<i64>,

    /// Estimated size in bytes of all the active files in this partition, or `None`
    /// if the partition size has not been computed yet.
    pub estimated_size_bytes: Option<i64>,
}

impl Partition {
    /// Create a new Partition data object from the given attributes. This constructor will take
    /// care of computing the [`PartitionHashId`].
    ///
    /// This is only appropriate to use in the catalog or in tests.
    #[expect(clippy::too_many_arguments)]
    pub fn new_catalog_only(
        id: PartitionId,
        table_id: TableId,
        partition_key: PartitionKey,
        sort_key_ids: SortKeyIds,
        new_file_at: Option<Timestamp>,
        cold_compact_at: Option<Timestamp>,
        created_at: Option<Timestamp>,
        max_time: Option<i64>,
        estimated_size_bytes: Option<i64>,
    ) -> Self {
        Self {
            id,
            hash_id: PartitionHashId::new(table_id, &partition_key),
            table_id,
            partition_key,
            sort_key_ids,
            new_file_at,
            cold_compact_at,
            created_at,
            max_time,
            estimated_size_bytes,
        }
    }

    /// The unique hash derived from the table ID and partition key
    pub fn hash_id(&self) -> &PartitionHashId {
        &self.hash_id
    }

    /// The sort key IDs, if the sort key has been set
    pub fn sort_key_ids(&self) -> Option<&SortKeyIds> {
        if self.sort_key_ids.is_empty() {
            None
        } else {
            Some(&self.sort_key_ids)
        }
    }

    /// The sort key containing the column names found in the specified column map.
    ///
    /// # Panics
    ///
    /// Will panic if an ID isn't found in the column map.
    pub fn sort_key(&self, columns_by_name: &ColumnsByName) -> Option<SortKey> {
        self.sort_key_ids()
            .map(|sort_key_ids| sort_key_ids.to_sort_key(columns_by_name))
    }

    /// Change the sort key IDs to the given sort key IDs. This should only be used in the
    /// in-memory catalog or in tests; all other sort key updates should go through the catalog
    /// functions.
    pub fn set_sort_key_ids(&mut self, sort_key_ids: &SortKeyIds) {
        self.sort_key_ids = sort_key_ids.clone();
    }

    /// Read access to the created_at time. This should be set by the catalog.
    pub fn created_at(&self) -> Option<Timestamp> {
        self.created_at
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::hash::{Hash, Hasher};

    use super::*;

    use proptest::proptest;

    /// A fixture test asserting the partition hash generation
    /// algorithm outputs a fixed value, preventing accidental changes to the
    /// derived ID.
    ///
    /// This hash byte value MUST NOT change for the lifetime of a cluster
    /// (though the encoding used in this test can).
    #[test]
    fn display_partition_hash_id_in_hex() {
        let partition_hash_id =
            PartitionHashId::new(TableId::new(5), &PartitionKey::from("2023-06-08"));

        assert_eq!(
            "ebd1041daa7c644c99967b817ae607bdcb754c663f2c415f270d6df720280f7a",
            partition_hash_id.to_string()
        );
    }

    proptest! {
        #[test]
        fn partition_hash_id_representations(
            table_id in 0..i64::MAX,
            partition_key in ".+",
        ) {
            let table_id = TableId::new(table_id);
            let partition_key = PartitionKey::from(partition_key);

            let partition_hash_id = PartitionHashId::new(table_id, &partition_key);

            // ID generation MUST be deterministic.
            let partition_hash_id_regenerated = PartitionHashId::new(table_id, &partition_key);
            assert_eq!(partition_hash_id, partition_hash_id_regenerated);

            // ID generation MUST be collision resistant; different inputs -> different IDs
            let other_table_id = TableId::new(table_id.get().wrapping_add(1));
            let different_partition_hash_id = PartitionHashId::new(other_table_id, &partition_key);
            assert_ne!(partition_hash_id, different_partition_hash_id);

            // The bytes of the partition hash ID are stored in the catalog and sent from the
            // ingesters to the queriers. We should be able to round-trip through bytes.
            let bytes_representation = partition_hash_id.as_bytes();
            assert_eq!(bytes_representation.len(), 32);
            let from_bytes = PartitionHashId::try_from(bytes_representation).unwrap();
            assert_eq!(from_bytes, partition_hash_id);

            // The hex string of the bytes is used in the Parquet file path in object storage, and
            // should always be the same length.
            let string_representation = partition_hash_id.to_string();
            assert_eq!(string_representation.len(), 64);

            // While nothing is currently deserializing the hex string to create `PartitionHashId`
            // instances, it should work because there's nothing preventing it either.
            let bytes_from_string = hex::decode(string_representation).unwrap();
            let from_string = PartitionHashId::try_from(&bytes_from_string[..]).unwrap();
            assert_eq!(from_string, partition_hash_id);
        }
    }

    #[test]
    fn test_hash_partition_hash_id() {
        let id = PartitionHashId::arbitrary_for_testing();

        let mut hasher = TestHasher::default();
        id.hash(&mut hasher);

        assert_eq!(hasher.written, vec![id.as_bytes()[..8].to_vec()],);
    }

    #[derive(Debug, Default)]
    struct TestHasher {
        written: Vec<Vec<u8>>,
    }

    impl Hasher for TestHasher {
        fn finish(&self) -> u64 {
            unimplemented!()
        }

        fn write(&mut self, bytes: &[u8]) {
            self.written.push(bytes.to_vec());
        }
    }
}
