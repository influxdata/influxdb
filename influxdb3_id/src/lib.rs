use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::fmt::Display;
use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

mod serialize;
pub use serialize::{SerdeVecMap, SerdeVecSet};

pub trait CatalogId: Default + Hash + Eq + Copy + Ord + Serialize {
    type Integer;

    const MAX: Self;

    fn next(&self) -> Self;
}

#[derive(Debug, thiserror::Error)]
#[error("failed to parse as integer: {0}")]
pub struct IdParseError(#[from] ParseIntError);

macro_rules! catalog_identifier_type {
    ($name:ident, $ty:ty) => {
        #[derive(
            Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash,
        )]
        pub struct $name($ty);

        impl CatalogId for $name {
            type Integer = $ty;

            const MAX: Self = Self(<$ty>::MAX);

            fn next(&self) -> Self {
                Self::new(self.0.checked_add(1).expect("incrementing id overflow"))
            }
        }

        impl $name {
            pub const fn new(id: $ty) -> Self {
                Self(id)
            }

            pub const fn get(&self) -> $ty {
                self.0
            }
        }

        impl From<$ty> for $name {
            fn from(int: $ty) -> Self {
                Self::new(int)
            }
        }

        impl Default for $name {
            /// The default for any identifier type is 0
            fn default() -> Self {
                Self::new(0)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse().map(Self).map_err(Into::into)
            }
        }
    };
}

catalog_identifier_type!(NodeId, u32);
catalog_identifier_type!(DbId, u32);
catalog_identifier_type!(TableId, u32);
catalog_identifier_type!(TriggerId, u32);
catalog_identifier_type!(ColumnId, u16);
catalog_identifier_type!(TagId, u8);
catalog_identifier_type!(FieldFamilyId, u16);
catalog_identifier_type!(FieldId, u16);
catalog_identifier_type!(LastCacheId, u16);
catalog_identifier_type!(DistinctCacheId, u16);
catalog_identifier_type!(TokenId, u64);

/// The next file id to be used when persisting `ParquetFile`s
pub static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone, PartialOrd, Ord, Hash)]
/// A newtype wrapper for ids used with `ParquetFile`
pub struct ParquetFileId(u64);

impl ParquetFileId {
    pub fn new() -> Self {
        Self(
            NEXT_FILE_ID
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
                .expect("Overflowed with Parquet File IDs"),
        )
    }

    pub fn next_id() -> Self {
        Self(NEXT_FILE_ID.load(Ordering::SeqCst))
    }

    pub fn set_next_id(&self) {
        NEXT_FILE_ID.store(self.0, Ordering::SeqCst)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for ParquetFileId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Default for ParquetFileId {
    fn default() -> Self {
        Self::new()
    }
}

/// Used for addressing into `TableIndex`-related collections.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIndexId {
    node_id: String,
    db_id: DbId,
    table_id: TableId,
}

impl TableIndexId {
    /// Create a new FullTableId
    pub fn new(node_id: impl Into<String>, db_id: DbId, table_id: TableId) -> Self {
        Self {
            node_id: node_id.into(),
            db_id,
            table_id,
        }
    }

    /// Get the node_id
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get the db_id
    pub fn db_id(&self) -> DbId {
        self.db_id
    }

    /// Get the table_id
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl fmt::Display for TableIndexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}, {:?}, {:?})",
            self.node_id, self.db_id, self.table_id
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FieldIdentifier(pub FieldFamilyId, pub FieldId);

impl<T, U> From<(T, U)> for FieldIdentifier
where
    T: Into<FieldFamilyId>,
    U: Into<FieldId>,
{
    fn from(value: (T, U)) -> Self {
        Self(value.0.into(), value.1.into())
    }
}

impl FieldIdentifier {
    pub fn new(field_family_id: impl Into<FieldFamilyId>, id: impl Into<FieldId>) -> Self {
        Self(field_family_id.into(), id.into())
    }

    pub fn field_family_id(&self) -> FieldFamilyId {
        self.0
    }

    pub fn id(&self) -> FieldId {
        self.1
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnIdentifier {
    Timestamp,
    Tag(TagId),
    Field(FieldIdentifier),
}

impl fmt::Display for ColumnIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp => write!(f, "timestamp"),
            Self::Tag(id) => write!(f, "tag({id})"),
            Self::Field(FieldIdentifier(ff_id, id)) => write!(f, "field({ff_id}, {id})"),
        }
    }
}

impl ColumnIdentifier {
    pub fn tag(id: impl Into<TagId>) -> Self {
        Self::Tag(id.into())
    }

    pub fn field(cf_id: impl Into<FieldFamilyId>, id: impl Into<FieldId>) -> Self {
        Self::Field(FieldIdentifier(cf_id.into(), id.into()))
    }

    pub fn to_tag(&self) -> TagId {
        match self {
            Self::Tag(id) => *id,
            _ => panic!("not a tag"),
        }
    }

    pub fn to_field(&self) -> FieldIdentifier {
        match self {
            Self::Field(id) => *id,
            _ => panic!("not a field"),
        }
    }
}

impl From<TagId> for ColumnIdentifier {
    fn from(id: TagId) -> Self {
        Self::Tag(id)
    }
}

impl From<&TagId> for ColumnIdentifier {
    fn from(id: &TagId) -> Self {
        Self::Tag(*id)
    }
}
