//! Types having to do with partitions.

use crate::SortedColumnSet;

use super::{TableId, Timestamp};

use schema::sort::SortKey;
use sha2::Digest;
use std::{fmt::Display, sync::Arc};
use thiserror::Error;

/// Unique ID for a `Partition` during the transition from catalog-assigned sequential
/// `PartitionId`s to deterministic `PartitionHashId`s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransitionPartitionId {
    /// The old catalog-assigned sequential `PartitionId`s that are in the process of being
    /// deprecated.
    Deprecated(PartitionId),
    /// The new deterministic, hash-based `PartitionHashId`s that will be the new way to identify
    /// partitions.
    Deterministic(PartitionHashId),
}

impl TransitionPartitionId {
    /// Size in bytes including `self`.
    pub fn size(&self) -> usize {
        match self {
            Self::Deprecated(_) => std::mem::size_of::<Self>(),
            Self::Deterministic(id) => {
                std::mem::size_of::<Self>() + id.size() - std::mem::size_of_val(id)
            }
        }
    }
}

impl<'a, R> sqlx::FromRow<'a, R> for TransitionPartitionId
where
    R: sqlx::Row,
    &'static str: sqlx::ColumnIndex<R>,
    PartitionId: sqlx::decode::Decode<'a, R::Database>,
    PartitionId: sqlx::types::Type<R::Database>,
    Option<PartitionHashId>: sqlx::decode::Decode<'a, R::Database>,
    Option<PartitionHashId>: sqlx::types::Type<R::Database>,
{
    fn from_row(row: &'a R) -> sqlx::Result<Self> {
        let partition_id: Option<PartitionId> = row.try_get("partition_id")?;
        let partition_hash_id: Option<PartitionHashId> = row.try_get("partition_hash_id")?;

        let transition_partition_id = match (partition_id, partition_hash_id) {
            (_, Some(hash_id)) => TransitionPartitionId::Deterministic(hash_id),
            (Some(id), _) => TransitionPartitionId::Deprecated(id),
            (None, None) => {
                return Err(sqlx::Error::ColumnDecode {
                    index: "partition_id".into(),
                    source: "Both partition_id and partition_hash_id were NULL".into(),
                })
            }
        };

        Ok(transition_partition_id)
    }
}

impl From<(PartitionId, Option<&PartitionHashId>)> for TransitionPartitionId {
    fn from((partition_id, partition_hash_id): (PartitionId, Option<&PartitionHashId>)) -> Self {
        partition_hash_id
            .cloned()
            .map(TransitionPartitionId::Deterministic)
            .unwrap_or_else(|| TransitionPartitionId::Deprecated(partition_id))
    }
}

impl std::fmt::Display for TransitionPartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Deprecated(old_partition_id) => write!(f, "{}", old_partition_id.0),
            Self::Deterministic(partition_hash_id) => write!(f, "{}", partition_hash_id),
        }
    }
}

impl TransitionPartitionId {
    /// Create a new `TransitionPartitionId::Deterministic` with the given table
    /// ID and partition key. Provided to reduce typing and duplication a bit,
    /// and because this variant should be most common now.
    ///
    /// This MUST NOT be used for partitions that are addressed using legacy /
    /// deprecated catalog row IDs, which should use
    /// [`TransitionPartitionId::Deprecated`] instead.
    pub fn new(table_id: TableId, partition_key: &PartitionKey) -> Self {
        Self::Deterministic(PartitionHashId::new(table_id, partition_key))
    }

    /// Create a new `TransitionPartitionId` for cases in tests where you need some value but the
    /// value doesn't matter. Public and not test-only so that other crates' tests can use this.
    pub fn arbitrary_for_testing() -> Self {
        Self::new(TableId::new(0), &PartitionKey::from("arbitrary"))
    }
}

/// Errors deserialising protobuf representations of [`TransitionPartitionId`].
#[derive(Debug, Error)]
pub enum PartitionIdProtoError {
    /// The proto type does not contain an ID.
    #[error("no id specified for partition id")]
    NoId,

    /// The specified hash ID is invalid.
    #[error(transparent)]
    InvalidHashId(#[from] PartitionHashIdError),
}

/// Serialise a [`TransitionPartitionId`] to a protobuf representation.
impl From<TransitionPartitionId>
    for generated_types::influxdata::iox::catalog::v1::PartitionIdentifier
{
    fn from(value: TransitionPartitionId) -> Self {
        use generated_types::influxdata::iox::catalog::v1 as proto;
        match value {
            TransitionPartitionId::Deprecated(id) => proto::PartitionIdentifier {
                id: Some(proto::partition_identifier::Id::CatalogId(id.get())),
            },
            TransitionPartitionId::Deterministic(hash) => proto::PartitionIdentifier {
                id: Some(proto::partition_identifier::Id::HashId(
                    hash.as_bytes().to_owned(),
                )),
            },
        }
    }
}

/// Deserialise a [`TransitionPartitionId`] from a protobuf representation.
impl TryFrom<generated_types::influxdata::iox::catalog::v1::PartitionIdentifier>
    for TransitionPartitionId
{
    type Error = PartitionIdProtoError;

    fn try_from(
        value: generated_types::influxdata::iox::catalog::v1::PartitionIdentifier,
    ) -> Result<Self, Self::Error> {
        use generated_types::influxdata::iox::catalog::v1 as proto;

        let id = value.id.ok_or(PartitionIdProtoError::NoId)?;

        Ok(match id {
            proto::partition_identifier::Id::CatalogId(v) => {
                TransitionPartitionId::Deprecated(PartitionId::new(v))
            }
            proto::partition_identifier::Id::HashId(hash) => {
                TransitionPartitionId::Deterministic(PartitionHashId::try_from(hash.as_slice())?)
            }
        })
    }
}

/// Unique ID for a `Partition`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, sqlx::FromRow)]
#[sqlx(transparent)]
pub struct PartitionId(i64);

#[allow(missing_docs)]
impl PartitionId {
    pub const fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

impl sqlx::Encode<'_, sqlx::Postgres> for PartitionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode(&self.0, buf)
    }
}

impl sqlx::Decode<'_, sqlx::Postgres> for PartitionKey {
    fn decode(
        value: <sqlx::Postgres as sqlx::database::HasValueRef<'_>>::ValueRef,
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
        buf: &mut <sqlx::Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <String as sqlx::Encode<sqlx::Sqlite>>::encode(self.0.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::Sqlite> for PartitionKey {
    fn decode(
        value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <String as sqlx::Decode<sqlx::Sqlite>>::decode(value)?.into(),
        ))
    }
}

const PARTITION_HASH_ID_SIZE_BYTES: usize = 32;

/// Uniquely identify a partition based on its table ID and partition key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::FromRow)]
#[sqlx(transparent)]
pub struct PartitionHashId(Arc<[u8; PARTITION_HASH_ID_SIZE_BYTES]>);

impl std::fmt::Display for PartitionHashId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for byte in &*self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Reasons bytes specified aren't a valid `PartitionHashId`.
#[derive(Debug, Error)]
#[allow(missing_copy_implementations)]
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

        inner.update(partition_key.as_bytes());
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

impl<'q> sqlx::encode::Encode<'q, sqlx::Postgres> for &'q PartitionHashId {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        buf.extend_from_slice(self.0.as_ref());

        sqlx::encode::IsNull::No
    }
}

impl<'q> sqlx::encode::Encode<'q, sqlx::Sqlite> for &'q PartitionHashId {
    fn encode_by_ref(
        &self,
        args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>,
    ) -> sqlx::encode::IsNull {
        args.push(sqlx::sqlite::SqliteArgumentValue::Blob(
            std::borrow::Cow::Borrowed(self.0.as_ref()),
        ));

        sqlx::encode::IsNull::No
    }
}

impl<'r, DB: ::sqlx::Database> ::sqlx::decode::Decode<'r, DB> for PartitionHashId
where
    &'r [u8]: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as ::sqlx::database::HasValueRef<'r>>::ValueRef,
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

/// Data object for a partition. The combination of table and key are unique (i.e. only one record
/// can exist for each combo)
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow, Hash)]
pub struct Partition {
    /// the id of the partition
    pub id: PartitionId,
    /// The unique hash derived from the table ID and partition key, if available. This will become
    /// required when partitions without the value have aged out.
    hash_id: Option<PartitionHashId>,
    /// the table the partition is under
    pub table_id: TableId,
    /// the string key of the partition
    pub partition_key: PartitionKey,

    // TODO: remove this field once the sort_key_ids is fully imlemented
    /// vector of column names that describes how *every* parquet file
    /// in this [`Partition`] is sorted.
    pub sort_key: Vec<String>,

    /// vector of column ids that describes how *every* parquet file
    /// in this [`Partition`] is sorted. The sort_key contains all the
    /// primary key (PK) columns that have been persisted, and nothing
    /// else. The PK columns are all `tag` columns and the `time`
    /// column.
    ///
    /// Even though it is possible for both the unpersisted data
    /// and/or multiple parquet files to contain different subsets of
    /// columns, the partition's sort_key is guaranteed to be
    /// "compatible" across all files. Compatible means that the
    /// parquet file is sorted in the same order as the partition
    /// sort_key after removing any missing columns.
    ///
    /// Partitions are initially created before any data is persisted
    /// with an empty sort_key. The partition sort_key is updated as
    /// needed when data is persisted to parquet files: both on the
    /// first persist when the sort key is empty, as on subsequent
    /// persist operations when new tags occur in newly inserted data.
    ///
    /// Updating inserts new column into the existing order. The order
    /// of the existing columns relative to each other is NOT changed.
    ///
    /// For example, updating `A,B,C` to either `A,D,B,C` or `A,B,C,D`
    /// is legal. However, updating to `A,C,D,B` is not because the
    /// relative order of B and C have been reversed.
    pub sort_key_ids: Option<SortedColumnSet>,

    /// The time at which the newest file of the partition is created
    pub new_file_at: Option<Timestamp>,
}

impl Partition {
    /// Create a new Partition data object from the given attributes. This constructor will take
    /// care of computing the [`PartitionHashId`].
    ///
    /// This is only appropriate to use in the in-memory catalog or in tests.
    pub fn new_in_memory_only(
        id: PartitionId,
        table_id: TableId,
        partition_key: PartitionKey,
        sort_key: Vec<String>,
        sort_key_ids: Option<SortedColumnSet>,
        new_file_at: Option<Timestamp>,
    ) -> Self {
        let hash_id = PartitionHashId::new(table_id, &partition_key);
        Self {
            id,
            hash_id: Some(hash_id),
            table_id,
            partition_key,
            sort_key,
            sort_key_ids,
            new_file_at,
        }
    }

    /// The sqlite catalog has to define a `PartitionPod` type that's slightly different than
    /// `Partition` because of what sqlite serialization is supported. This function is for
    /// conversion between the `PartitionPod` type and `Partition` and should not be used anywhere
    /// else.
    ///
    /// The in-memory catalog also creates the `Partition` directly from w
    pub fn new_with_hash_id_from_sqlite_catalog_only(
        id: PartitionId,
        hash_id: Option<PartitionHashId>,
        table_id: TableId,
        partition_key: PartitionKey,
        sort_key: Vec<String>,
        sort_key_ids: Option<SortedColumnSet>,
        new_file_at: Option<Timestamp>,
    ) -> Self {
        Self {
            id,
            hash_id,
            table_id,
            partition_key,
            sort_key,
            sort_key_ids,
            new_file_at,
        }
    }

    /// If this partition has a `PartitionHashId` stored in the catalog, use that. Otherwise, use
    /// the database-assigned `PartitionId`.
    pub fn transition_partition_id(&self) -> TransitionPartitionId {
        TransitionPartitionId::from((self.id, self.hash_id.as_ref()))
    }

    /// The unique hash derived from the table ID and partition key, if it exists in the catalog.
    pub fn hash_id(&self) -> Option<&PartitionHashId> {
        self.hash_id.as_ref()
    }

    // TODO: remove this function after all PRs that teach compactor, ingester,
    // and querier to use sort_key_ids are merged.
    /// The sort key for the partition, if present, structured as a `SortKey`
    pub fn sort_key(&self) -> Option<SortKey> {
        if self.sort_key.is_empty() {
            return None;
        }

        Some(SortKey::from_columns(self.sort_key.iter().map(|s| &**s)))
    }

    /// The sort_key_ids if present
    pub fn sort_key_ids(&self) -> Option<&SortedColumnSet> {
        self.sort_key_ids.as_ref()
    }

    /// The sort_key_ids if present and not empty
    pub fn sort_key_ids_none_if_empty(&self) -> Option<SortedColumnSet> {
        match self.sort_key_ids.as_ref() {
            None => None,
            Some(sort_key_ids) => {
                if sort_key_ids.is_empty() {
                    None
                } else {
                    Some(sort_key_ids.clone())
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use proptest::{prelude::*, proptest};

    /// A fixture test asserting the deterministic partition ID generation
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

    prop_compose! {
        /// Return an arbitrary [`TransitionPartitionId`] with a randomised ID
        /// value.
        pub fn arbitrary_partition_id()(
            use_hash in any::<bool>(),
            row_id in any::<i64>(),
            hash_id in any::<[u8; PARTITION_HASH_ID_SIZE_BYTES]>()
        ) -> TransitionPartitionId {
            match use_hash {
                true => TransitionPartitionId::Deterministic(PartitionHashId(hash_id.into())),
                false => TransitionPartitionId::Deprecated(PartitionId::new(row_id)),
            }
        }
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

        /// Assert a [`TransitionPartitionId`] is round-trippable through proto
        /// serialisation.
        #[test]
        fn prop_partition_id_proto_round_trip(id in arbitrary_partition_id()) {
            use generated_types::influxdata::iox::catalog::v1 as proto;

            // Encoding is infallible
            let encoded = proto::PartitionIdentifier::from(id.clone());

            // Decoding a valid ID is infallible.
            let decoded = TransitionPartitionId::try_from(encoded).unwrap();

            // The deserialised value must match the input (round trippable)
            assert_eq!(decoded, id);
        }
    }

    #[test]
    fn test_proto_no_id() {
        use generated_types::influxdata::iox::catalog::v1 as proto;

        let msg = proto::PartitionIdentifier { id: None };

        assert_matches!(
            TransitionPartitionId::try_from(msg),
            Err(PartitionIdProtoError::NoId)
        );
    }

    #[test]
    fn test_proto_bad_hash() {
        use generated_types::influxdata::iox::catalog::v1 as proto;

        let msg = proto::PartitionIdentifier {
            id: Some(proto::partition_identifier::Id::HashId(vec![42])),
        };

        assert_matches!(
            TransitionPartitionId::try_from(msg),
            Err(PartitionIdProtoError::InvalidHashId(_))
        );
    }
}
