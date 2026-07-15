//! Multi-record catalog transactions committed atomically under optimistic
//! concurrency.
//!
//! Callers `begin()` a transaction, push records into it, then `commit()`. On
//! `Prompt::Retry`, the caller rebuilds the transaction against the refreshed
//! catalog state — records accumulated in a transaction may no longer be
//! valid after another writer has advanced the catalog.
//!
//! [`DatabaseCatalogTransaction`] wraps [`CatalogTransaction`] with a
//! per-database view and per-table accumulators used by the schema-on-write
//! path during line-protocol parsing.

use std::sync::Arc;

use hashbrown::HashMap;
use influxdb3_id::{ColumnId, FieldFamilyId, FieldId, FieldIdentifier, TableId};
use observability_deps::tracing::trace;
use schema::{InfluxColumnType, InfluxFieldType};

use crate::catalog::versions::v3::schema::column::{
    ColumnDefinition, FieldColumn, FieldDataType, FieldFamilyDefinition, FieldFamilyMode,
    FieldFamilyName, TagColumn, TimestampColumn,
};
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::catalog::versions::v3::{NUM_FIELD_FAMILIES_LIMIT, NUM_FIELDS_PER_FAMILY_LIMIT};
use crate::catalog::{CatalogSequenceNumber, CreateTableOptions, RetentionPeriod};
use crate::error::TruncatedTableName;
use crate::format::records::types::{
    ColumnDefinition as WireColumnDef, FieldColumn as WireFieldColumn,
    FieldDataType as WireFieldDataType, FieldFamilyDefinition as WireFieldFamilyDef,
    FieldFamilyName as WireFieldFamilyName, FieldIdentifier as WireFieldIdentifier,
    RetentionPeriod as WireRetentionPeriod, TagColumn as WireTagColumn,
    TimestampColumn as WireTimestampColumn,
};
use crate::format::records::{AddColumns, CreateTable};
use crate::format::{CatalogRecord, RecordBatch};
use crate::{CatalogError, Result};

/// Convert the schema-level [`StorageMode`] to the log-level one used by
/// [`CatalogError::LegacyColumnIdsExhausted`].
// TODO(tjh): when the v2 catalog is swapped for the v3 catalog in the application, this dependency
// should be removed, i.e., there should not be a dependency on the old log::v4::StorageMode, but
// instead on the catalog::versions::v3::schema::StorageMode type.
fn log_storage_mode(mode: StorageMode) -> crate::log::versions::v4::StorageMode {
    match mode {
        StorageMode::Parquet => crate::log::versions::v4::StorageMode::Parquet,
        StorageMode::PachaTree => crate::log::versions::v4::StorageMode::PachaTree,
        StorageMode::ParquetAndPachaTree => {
            crate::log::versions::v4::StorageMode::ParquetAndPachaTree
        }
    }
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

/// Outcome of a transaction commit under optimistic concurrency.
///
/// Generic over `Success` and `Retry` payloads to mirror v2 — most callers
/// use the default `Prompt<CatalogSequenceNumber, ()>` returned by
/// [`Catalog::commit`][super::catalog::Catalog::commit], but write-buffer
/// validators thread their own state through both variants.
#[derive(Debug, Clone, Copy)]
pub enum Prompt<Success = (), Retry = ()> {
    Success(Success),
    Retry(Retry),
}

impl<S, R> Prompt<S, R> {
    /// Extract the `Success` payload, panicking if this is a `Retry`.
    /// Convenience for tests / call sites that have already filtered.
    pub fn unwrap_success(self) -> S {
        let Self::Success(s) = self else {
            panic!("tried to unwrap a retry as success");
        };
        s
    }
}

// ---------------------------------------------------------------------------
// CatalogTransaction
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CatalogTransaction {
    pub(super) sequence_at_begin: CatalogSequenceNumber,
    pub(super) records: RecordBatch,
}

impl CatalogTransaction {
    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence_at_begin
    }

    pub fn is_empty(&self) -> bool {
        self.records.as_slice().is_empty()
    }

    pub fn push<R: CatalogRecord>(&mut self, record: &R) {
        self.records.push(record);
    }
}

// ---------------------------------------------------------------------------
// DatabaseCatalogTransaction
// ---------------------------------------------------------------------------

/// Per-database transaction used by the schema-on-write path. Accumulates new
/// tables and columns discovered while parsing a batch of line protocol and
/// commits them as a single atomic catalog write.
#[derive(Debug)]
pub struct DatabaseCatalogTransaction {
    inner: CatalogTransaction,
    database_schema: Arc<DatabaseSchema>,
    tables: HashMap<TableId, TableTransaction>,
    next_table_id: TableId,
    current_table_count: usize,
    table_limit: usize,
    columns_per_table_limit: usize,
    tag_columns_per_table_limit: usize,
    /// Determines how the legacy `ColumnId` is managed.
    storage_mode: StorageMode,
}

impl DatabaseCatalogTransaction {
    /// Construct a new transaction from a captured database schema snapshot.
    ///
    /// `current_table_count` is the total table count across the catalog at
    /// begin time. `table_limit` and `columns_per_table_limit` are resolved
    /// by the caller from the catalog's
    /// [`CatalogLimiter`][crate::catalog::versions::v3::usage::CatalogLimiter],
    /// so per-tier or per-deployment overrides apply.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        inner: CatalogTransaction,
        database_schema: Arc<DatabaseSchema>,
        next_table_id: TableId,
        current_table_count: usize,
        table_limit: usize,
        columns_per_table_limit: usize,
        tag_columns_per_table_limit: usize,
        storage_mode: StorageMode,
    ) -> Self {
        Self {
            inner,
            database_schema,
            tables: HashMap::new(),
            next_table_id,
            current_table_count,
            table_limit,
            columns_per_table_limit,
            tag_columns_per_table_limit,
            storage_mode,
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.sequence_number()
    }

    /// Snapshot of the database schema captured at begin time. Does not
    /// reflect uncommitted `table_or_create` / `column_or_create` mutations;
    /// observe new state via a fresh transaction after commit.
    pub fn db_schema(&self) -> &Arc<DatabaseSchema> {
        &self.database_schema
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
            && self
                .tables
                .values()
                .all(|tx| tx.new_columns.is_empty() && tx.new_field_families.is_empty())
    }

    /// Get-or-create a table. Uses [`FieldFamilyMode::Aware`], no retention.
    ///
    /// Aware mode is the write-path default (matching v2): a `family::field`
    /// field name routes to the named field family; unqualified names use the
    /// shared auto family.
    pub fn table_or_create(&mut self, table_name: &str) -> Result<TableId> {
        if let Some(id) = self.table_id_by_name(table_name) {
            return Ok(id);
        }
        self.create_table_with_opts(table_name, CreateTableOptions::default())
    }

    /// Create a table with an explicit field family mode. Errors if the
    /// table already exists (the mode of an existing table is not
    /// reconsidered).
    pub fn create_table_with_opts(
        &mut self,
        table_name: &str,
        options: CreateTableOptions,
    ) -> Result<TableId> {
        if self.table_id_by_name(table_name).is_some() {
            return Err(CatalogError::AlreadyExists);
        }

        if self.current_table_count >= self.table_limit {
            return Err(CatalogError::TooManyTables {
                current: self.current_table_count,
                limit: self.table_limit,
            });
        }

        let table_id = self.next_table_id;
        self.next_table_id = TableId::new(table_id.get() + 1);
        self.current_table_count += 1;

        let retention_period = match options.retention_period {
            Some(d) => WireRetentionPeriod::Duration {
                duration_secs: d.as_secs(),
            },
            None => WireRetentionPeriod::Indefinite,
        };

        self.inner.records.push(&CreateTable {
            database_id: self.database_schema.id.get(),
            database_name: self.database_schema.name.to_string(),
            table_name: table_name.to_string(),
            table_id: table_id.get(),
            retention_period,
            field_family_mode: (&options.field_family_mode).into(),
        });

        self.tables.insert(
            table_id,
            TableTransaction {
                table: TableDefinition::new_empty(
                    table_id,
                    Arc::from(table_name),
                    options.field_family_mode,
                ),
                new_columns: Vec::new(),
                new_field_families: Vec::new(),
                column_limit: self.columns_per_table_limit,
                tag_column_limit: self.tag_columns_per_table_limit,
                storage_mode: self.storage_mode,
                retention_period,
            },
        );

        Ok(table_id)
    }

    /// Get-or-create a table transaction handle.
    pub fn table_tx_or_create(&mut self, table_name: &str) -> Result<&mut TableTransaction> {
        let table_id = self.table_or_create(table_name)?;
        Ok(self
            .tables
            .get_mut(&table_id)
            .expect("table transaction should exist after table_or_create"))
    }

    /// Get-or-create a column on a named table. Errors if the column exists
    /// with a different type.
    pub fn column_or_create(
        &mut self,
        table_name: &str,
        column_name: &str,
        column_type: InfluxColumnType,
    ) -> Result<ColumnDefinition> {
        let table_id = self.table_or_create(table_name)?;
        let tx = self
            .tables
            .get_mut(&table_id)
            .expect("table should exist after table_or_create");

        if let Some(existing) = tx.table.column_definition(column_name) {
            if existing.column_type() != column_type {
                return Err(CatalogError::InvalidColumnType {
                    column_name: Arc::from(column_name),
                    expected: existing.column_type(),
                    got: column_type,
                });
            }
            return Ok(existing);
        }

        match column_type {
            InfluxColumnType::Timestamp => {
                let ts = tx.time_or_create()?;
                Ok(ColumnDefinition::Timestamp(ts))
            }
            InfluxColumnType::Tag => {
                let tag = tx.tag_or_create(column_name)?;
                Ok(ColumnDefinition::Tag(tag))
            }
            InfluxColumnType::Field(field_type) => {
                let field = tx.field_or_create(column_name, field_type)?;
                Ok(ColumnDefinition::Field(field))
            }
        }
    }

    /// Apply this transaction's records directly to a fresh `InnerCatalog`
    /// without persisting.
    pub fn apply_to_inner(
        &self,
        inner: &mut crate::catalog::versions::v3::inner::InnerCatalog,
    ) -> Result<Arc<DatabaseSchema>> {
        let sequence = inner.sequence_number().next();
        let mut batch = self.inner.records.clone();
        for (table_id, tx) in &self.tables {
            if !tx.new_columns.is_empty() || !tx.new_field_families.is_empty() {
                batch.push(&AddColumns {
                    database_id: self.database_schema.id.get(),
                    table_id: table_id.get(),
                    columns: tx.new_columns.clone(),
                    field_families: tx.new_field_families.clone(),
                });
            }
        }
        let _events = crate::format::apply::apply_records(
            batch.as_slice(),
            inner,
            sequence,
            &mut crate::format::apply::RestorePreload::empty(),
        )
        .map_err(|e| CatalogError::Internal {
            details: format!("apply_to_inner: {e}"),
        })?;
        inner
            .databases
            .get_by_id(&self.database_schema.id)
            .ok_or_else(|| CatalogError::NotFound(self.database_schema.name.to_string()))
    }

    /// Emit any pending per-table `AddColumns` records and return the inner
    /// [`CatalogTransaction`].
    pub fn finalize(mut self) -> CatalogTransaction {
        let db_id = self.database_schema.id.get();
        for (table_id, tx) in self.tables.drain() {
            if tx.new_columns.is_empty() && tx.new_field_families.is_empty() {
                continue;
            }
            self.inner.records.push(&AddColumns {
                database_id: db_id,
                table_id: table_id.get(),
                columns: tx.new_columns,
                field_families: tx.new_field_families,
            });
        }
        self.inner
    }

    /// Check that adding the incoming write columns won't exceed column limits.
    ///
    /// Computes projected counts for the full write by counting only columns
    /// that do not already exist in the table schema. Write validation calls
    /// this after a per-column limit breach to rewrite the error with the
    /// final projected count instead of "current + 1"; enforcement itself
    /// stays with the per-column checks so the success path pays nothing.
    pub fn check_write_column_limits(
        &mut self,
        table_name: &str,
        incoming_tags: &[&str],
        incoming_fields: &[&str],
    ) -> Result<()> {
        let column_limit = self.columns_per_table_limit;
        let tag_column_limit = self.tag_columns_per_table_limit;
        let Some(table_id) = self.table_id_by_name(table_name) else {
            return Ok(());
        };
        let table_tx = self
            .tables
            .get(&table_id)
            .expect("table transaction should exist after table_id_by_name");

        // Collect into sets so duplicate keys within a single line count once.
        let new_tags = incoming_tags
            .iter()
            .filter(|t| table_tx.table.column_definition(t).is_none())
            .collect::<std::collections::HashSet<_>>()
            .len();
        let new_fields = incoming_fields
            .iter()
            .filter(|f| table_tx.table.column_definition(f).is_none())
            .collect::<std::collections::HashSet<_>>()
            .len();

        let projected_tag_count = table_tx.table.num_tag_columns() + new_tags;
        let projected_total = table_tx.table.num_tag_columns()
            + table_tx.table.num_field_columns()
            + new_tags
            + new_fields;

        if projected_tag_count > tag_column_limit {
            return Err(CatalogError::TooManyTagColumns {
                table_name: TruncatedTableName::new(Arc::clone(&table_tx.table.table_name)),
                attempted: projected_tag_count,
                limit: tag_column_limit,
            });
        }
        if projected_total > column_limit {
            return Err(CatalogError::TooManyColumns {
                table_name: TruncatedTableName::new(Arc::clone(&table_tx.table.table_name)),
                attempted: projected_total,
                limit: column_limit,
            });
        }

        Ok(())
    }

    fn table_id_by_name(&mut self, table_name: &str) -> Option<TableId> {
        for (id, tx) in &self.tables {
            if tx.table.table_name.as_ref() == table_name {
                return Some(*id);
            }
        }

        if let Some(table_def) = self.database_schema.table_definition(table_name) {
            let table_id = table_def.table_id;
            self.tables.entry(table_id).or_insert_with(|| {
                TableTransaction::from_existing(
                    table_def.as_ref().clone(),
                    self.columns_per_table_limit,
                    self.tag_columns_per_table_limit,
                    self.storage_mode,
                )
            });
            return Some(table_id);
        }

        None
    }
}

// ---------------------------------------------------------------------------
// TableTransaction
// ---------------------------------------------------------------------------

/// Per-table accumulator — the evolving [`TableDefinition`] plus a pending set
/// of `AddColumns` wire payloads. Field additions respect the table's
/// [`FieldFamilyMode`]: in `Aware` mode a `family::field` name routes to the
/// named family (created lazily), while unqualified names and `Auto`-mode
/// tables use the shared auto family.
#[derive(Debug)]
pub struct TableTransaction {
    pub(crate) table: TableDefinition,
    new_columns: Vec<WireColumnDef>,
    new_field_families: Vec<WireFieldFamilyDef>,
    column_limit: usize,
    tag_column_limit: usize,
    storage_mode: StorageMode,
    retention_period: WireRetentionPeriod,
}

impl TableTransaction {
    fn from_existing(
        table: TableDefinition,
        column_limit: usize,
        tag_column_limit: usize,
        storage_mode: StorageMode,
    ) -> Self {
        Self {
            retention_period: match table.retention_period {
                RetentionPeriod::Indefinite => WireRetentionPeriod::Indefinite,
                RetentionPeriod::Duration(ref duration) => WireRetentionPeriod::Duration {
                    duration_secs: duration.as_secs(),
                },
            },
            table,
            new_columns: Vec::new(),
            new_field_families: Vec::new(),
            column_limit,
            tag_column_limit,
            storage_mode,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table.table_id
    }

    pub fn num_columns(&self) -> usize {
        self.table.num_columns()
    }

    pub fn num_field_families(&self) -> usize {
        self.table.num_field_families()
    }

    #[inline]
    fn check_columns_limit(&self) -> Result<()> {
        trace!(
            n_tags = self.table.num_tag_columns(),
            n_fields = self.table.num_field_columns(),
            total = self.table.num_tag_columns() + self.table.num_field_columns(),
            limit = self.column_limit,
            "check column limit",
        );
        let current = self.table.num_tag_columns() + self.table.num_field_columns();
        if current >= self.column_limit {
            Err(CatalogError::TooManyColumns {
                table_name: TruncatedTableName::new(Arc::clone(&self.table.table_name)),
                attempted: current + 1,
                limit: self.column_limit,
            })
        } else {
            Ok(())
        }
    }

    #[inline]
    fn check_field_family_limit(&self) -> Result<()> {
        if self.num_field_families() >= NUM_FIELD_FAMILIES_LIMIT {
            Err(CatalogError::TooManyFieldFamilies(NUM_FIELD_FAMILIES_LIMIT))
        } else {
            Ok(())
        }
    }

    pub fn time_or_create(&mut self) -> Result<Arc<TimestampColumn>> {
        if let Some(existing) = self.table.column_definition("time")
            && let ColumnDefinition::Timestamp(ts) = existing
        {
            return Ok(ts);
        }
        // The timestamp column is exempt from the per-table column limit, so a
        // table may hold `column_limit` tag/field columns plus `time`. Matches
        // v2, which omits this check from `add_time`.
        let col_def = self.add_time()?;
        match col_def {
            ColumnDefinition::Timestamp(ts) => Ok(ts),
            _ => unreachable!(),
        }
    }

    pub fn tag_or_create(&mut self, name: &str) -> Result<Arc<TagColumn>> {
        if let Some(existing) = self.table.column_definition(name) {
            match existing {
                ColumnDefinition::Tag(tag) => return Ok(tag),
                other => {
                    return Err(CatalogError::InvalidColumnType {
                        column_name: Arc::from(name),
                        expected: InfluxColumnType::Tag,
                        got: other.column_type(),
                    });
                }
            }
        }
        let col_def = self.add_tag(name)?;
        match col_def {
            ColumnDefinition::Tag(tag) => Ok(tag),
            _ => unreachable!(),
        }
    }

    /// Add or re-use a field column.
    ///
    /// In [`FieldFamilyMode::Auto`], the field is placed in the shared auto
    /// field family. In [`FieldFamilyMode::Aware`], a `family::field` name
    /// routes the field to the named field family (created lazily);
    /// unqualified names fall back to the auto family. Malformed qualifiers
    /// (empty family name or empty field part) are treated as unqualified.
    pub fn field_or_create(
        &mut self,
        name: &str,
        field_type: InfluxFieldType,
    ) -> Result<Arc<FieldColumn>> {
        if let Some(existing) = self.table.column_definition(name) {
            match existing {
                ColumnDefinition::Field(f) if f.data_type == field_type => return Ok(f),
                other => {
                    return Err(CatalogError::InvalidColumnType {
                        column_name: Arc::from(name),
                        expected: InfluxColumnType::Field(field_type),
                        got: other.column_type(),
                    });
                }
            }
        }
        let col_def = self.add_field(name, field_type)?;
        match col_def {
            ColumnDefinition::Field(f) => Ok(f),
            _ => unreachable!(),
        }
    }

    /// Allocate the next legacy `ColumnId`
    ///
    /// PachaTree carries `None` once the `u16` space is exhausted (it
    /// addresses columns by `ColumnIdentifier`); Parquet/`ParquetAndPachaTree`
    /// error, since their read paths project by ordinal. Peeks only — the
    /// counter advances when `ColumnSet::insert` takes the new column.
    fn next_legacy_column_id(&self) -> Result<Option<ColumnId>> {
        let next = self.table.columns.next_id;
        match self.storage_mode {
            StorageMode::PachaTree => Ok(next),
            StorageMode::Parquet | StorageMode::ParquetAndPachaTree => {
                next.map(Some)
                    .ok_or_else(|| CatalogError::LegacyColumnIdsExhausted {
                        table_name: Arc::clone(&self.table.table_name),
                        storage_mode: log_storage_mode(self.storage_mode),
                    })
            }
        }
    }

    fn add_time(&mut self) -> Result<ColumnDefinition> {
        let col_id = self.next_legacy_column_id()?;
        let col_def = ColumnDefinition::Timestamp(Arc::new(TimestampColumn {
            column_id: col_id,
            name: "time".into(),
        }));
        self.new_columns
            .push(WireColumnDef::Timestamp(WireTimestampColumn {
                column_id: col_id.map(|c| c.get()),
                name: "time".to_string(),
            }));
        self.table
            .add_columns_to_maps(vec![col_def.clone()])
            .expect("adding timestamp column should not fail");
        Ok(col_def)
    }

    fn add_tag(&mut self, name: &str) -> Result<ColumnDefinition> {
        self.table.check_name(name)?;
        if self.table.tag_columns.len() >= self.tag_column_limit {
            return Err(CatalogError::TooManyTagColumns {
                table_name: TruncatedTableName::new(Arc::clone(&self.table.table_name)),
                attempted: self.table.tag_columns.len() + 1,
                limit: self.tag_column_limit,
            });
        }
        self.check_columns_limit()?;
        let col_id = self.next_legacy_column_id()?;
        let tag_id = self.table.tag_columns.next_id();
        let col_def = ColumnDefinition::Tag(Arc::new(TagColumn {
            id: tag_id,
            column_id: col_id,
            name: name.into(),
        }));
        self.new_columns.push(WireColumnDef::Tag(WireTagColumn {
            id: tag_id.get(),
            column_id: col_id.map(|c| c.get()),
            name: name.to_string(),
        }));
        self.table
            .add_columns_to_maps(vec![col_def.clone()])
            .expect("adding tag column should not fail");
        Ok(col_def)
    }

    fn add_field(&mut self, name: &str, field_type: InfluxFieldType) -> Result<ColumnDefinition> {
        self.table.check_name(name)?;
        let col_id = self.next_legacy_column_id()?;
        self.check_columns_limit()?;

        let ff_id = match self.table.field_family_mode {
            FieldFamilyMode::Auto => self.get_or_create_auto_field_family()?,
            FieldFamilyMode::Aware => match parse_qualified_field_name(name) {
                Some(family_name) => self.get_or_create_named_field_family(family_name)?,
                None => self.get_or_create_auto_field_family()?,
            },
        };
        let field_id = self
            .table
            .fields_by_family(ff_id)
            .map(|fields| fields.next_id())
            .unwrap_or_else(|| FieldId::new(0));

        let field_identifier = FieldIdentifier::new(ff_id, field_id);
        let data_type = FieldDataType::from(field_type);
        let wire_data_type = WireFieldDataType::from(&data_type);

        let col_def = ColumnDefinition::Field(Arc::new(FieldColumn {
            id: field_identifier,
            column_id: col_id,
            name: name.into(),
            data_type: field_type,
        }));
        self.new_columns.push(WireColumnDef::Field(WireFieldColumn {
            id: WireFieldIdentifier {
                field_id: field_id.get(),
                family_id: ff_id.get(),
            },
            column_id: col_id.map(|c| c.get()),
            name: name.to_string(),
            data_type: wire_data_type,
        }));
        self.table
            .add_columns_to_maps(vec![col_def.clone()])
            .expect("adding field column should not fail");
        Ok(col_def)
    }

    fn get_or_create_named_field_family(&mut self, family_name: &str) -> Result<FieldFamilyId> {
        if let Some(existing) = self.table.field_families.get_by_name(family_name) {
            if let Some(fields) = self.table.fields_by_family(existing.id)
                && fields.len() >= NUM_FIELDS_PER_FAMILY_LIMIT
            {
                return Err(CatalogError::TooManyFields {
                    field_family: family_name.to_string(),
                    limit: NUM_FIELDS_PER_FAMILY_LIMIT,
                });
            }
            return Ok(existing.id);
        }

        self.check_field_family_limit()?;
        let ff_id = self.table.field_families.next_id();
        let ff_name = FieldFamilyName::User(Arc::from(family_name));
        let ff = FieldFamilyDefinition::new(ff_id, ff_name);
        self.table
            .field_families
            .insert(ff_id, ff)
            .expect("named field family insert should not fail");
        self.new_field_families.push(WireFieldFamilyDef {
            id: ff_id.get(),
            name: WireFieldFamilyName::User(family_name.to_string()),
        });
        Ok(ff_id)
    }

    fn get_or_create_auto_field_family(&mut self) -> Result<FieldFamilyId> {
        if let Some(ff_id) = self.table.auto_field_family() {
            let is_full = self
                .table
                .fields_by_family(ff_id)
                .is_some_and(|fields| fields.len() >= NUM_FIELDS_PER_FAMILY_LIMIT);
            if !is_full {
                return Ok(ff_id);
            }
        }

        self.check_field_family_limit()?;
        let ff_id = self.table.field_families.next_id();
        let auto_name = self.table.next_auto_field_family_name();
        let ff_name = FieldFamilyName::Auto(auto_name);
        let ff = FieldFamilyDefinition::new(ff_id, ff_name);
        self.table
            .field_families
            .insert(ff_id, ff)
            .expect("auto field family insert should not fail");
        self.table.set_auto_field_family(ff_id, auto_name)?;
        self.new_field_families.push(WireFieldFamilyDef {
            id: ff_id.get(),
            name: WireFieldFamilyName::Auto(auto_name),
        });
        Ok(ff_id)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse `family::field` to extract the family qualifier.
///
/// Returns `None` for unqualified names, or for malformed qualifiers (empty
/// family name or empty field part) — callers treat these as unqualified.
pub(crate) fn parse_qualified_field_name(name: &str) -> Option<&str> {
    let (qualifier, field_part) = name.split_once("::")?;
    if qualifier.is_empty() || field_part.is_empty() {
        None
    } else {
        Some(qualifier)
    }
}

#[cfg(test)]
mod tests;
