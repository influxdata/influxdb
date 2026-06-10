use std::{iter, sync::Arc, time::Duration};

use hashbrown::HashMap;
use influxdb3_id::{
    ColumnIdentifier, DistinctCacheId, FieldFamilyId, FieldId, LastCacheId, TableId, TagId,
};
use iox_time::Time;
use schema::{InfluxColumnType, Schema, SchemaBuilder, sort::SortKey};

use crate::{
    CatalogError, Repository, Result,
    catalog::{
        DeletedSchema, IfNotDeleted, RESERVED_COLUMN_NAMES, TIME_COLUMN_NAME,
        versions::v3::{
            deletes::DeletionScope,
            schema::{
                cache::{DistinctCacheDefinition, LastCacheDefinition},
                column::{
                    ColumnDefinition, ColumnSet, FieldColumn, FieldFamilyDefinition,
                    FieldFamilyMode, FieldFamilyName, TagColumn, TimestampColumn,
                },
                retention::RetentionPeriod,
            },
        },
    },
    resource::CatalogResource,
};

/// Definition of a table in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    /// Unique identifier of the table in the catalog
    pub table_id: TableId,
    /// User-provided unique name for the table
    pub table_name: Arc<str>,
    /// The IOx/Arrow schema for the table
    pub schema: Schema,
    /// Ordered column definitions for the table
    pub columns: ColumnSet,
    /// The timestamp column for the table, if it exists
    timestamp_column: Option<Arc<TimestampColumn>>,
    /// Tag columns for the table
    pub tag_columns: Repository<TagId, TagColumn>,
    /// Field family definitions for the table
    pub field_families: Repository<FieldFamilyId, FieldFamilyDefinition>,
    field_count: usize,
    /// Fields which must be auto-assigned to a field family will be added to this one.
    auto_field_family: Option<FieldFamilyId>,
    /// The next auto field family name to be used when there is no existing `auto_field_family`.
    next_auto_field_family_name: u16,
    /// List of column identifiers that form the series key for the table
    ///
    /// The series key is used as the sort order, i.e., sort key, for the table during persistence.
    pub series_key: Vec<TagId>,
    /// The names of the columns in the table's series key
    pub series_key_names: Vec<Arc<str>>,
    /// The sort key for the table when persisted to storage.
    ///
    /// The sort key is the series key along with the `time` column form the primary key for the
    /// table. The series key is determined as the order of tags provided when the table is
    /// first created, either by a write of line protocol, or by an explicit table creation.
    pub sort_key: SortKey,
    /// Retention period for the table.
    pub retention_period: RetentionPeriod,
    /// Last cache definitions for the table
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    /// Distinct cache definitions for the table
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    /// Whether this table has been set as deleted
    pub deleted: bool,
    /// The time when the table is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
    /// The scope of the hard delete request, if any.
    pub hard_delete_scope: Option<DeletionScope>,
    pub field_family_mode: FieldFamilyMode,
}

impl CatalogResource for TableDefinition {
    type Identifier = TableId;

    const CATEGORY: &'static str = "tables";

    fn id(&self) -> Self::Identifier {
        self.table_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
}

impl TableDefinition {
    /// Create new empty `TableDefinition`
    pub fn new_empty(
        table_id: TableId,
        table_name: Arc<str>,
        field_family_mode: FieldFamilyMode,
    ) -> Self {
        Self::new(
            table_id,
            table_name,
            vec![],
            vec![],
            vec![],
            None,
            field_family_mode,
        )
        .expect("empty table should create without error")
    }

    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        column_defs: Vec<ColumnDefinition>,
        field_family_defs: Vec<(FieldFamilyId, FieldFamilyName)>,
        series_key: Vec<TagId>,
        retention_period: Option<Duration>,
        field_family_mode: FieldFamilyMode,
    ) -> Result<Self> {
        let mut field_families = Repository::new();
        let mut auto_field_family = None;
        let mut next_auto_field_family_name = 0;
        for (id, name) in field_family_defs {
            if let FieldFamilyName::Auto(v) = name
                && v >= next_auto_field_family_name
            {
                // It is not possible for the next auto ID overflow, because there is a
                // limit of u16::MAX field families per table, and the auto ID starts at 0.
                next_auto_field_family_name = v
                    .checked_add(1)
                    .ok_or_else(|| CatalogError::TooManyFieldFamilies(v.into()))?;
                auto_field_family = Some(id);
            }

            field_families
                .insert(id, FieldFamilyDefinition::new(id, name))
                .expect("field family definition should not exist");
        }

        let retention_period = match retention_period {
            Some(duration) => RetentionPeriod::Duration(duration),
            None => RetentionPeriod::Indefinite,
        };

        // Create an empty schema.
        let mut schema_builder = SchemaBuilder::with_capacity(column_defs.len());
        schema_builder.measurement(table_name.as_ref());
        let schema = schema_builder.build().expect("schema should be valid");

        let mut table_def = Self {
            table_id,
            table_name,
            schema,
            columns: ColumnSet::new(),
            timestamp_column: None,
            tag_columns: Repository::new(),
            field_families,
            field_count: 0,
            auto_field_family,
            next_auto_field_family_name,
            series_key: vec![],
            series_key_names: vec![],
            sort_key: SortKey::empty(),
            retention_period,
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
            hard_delete_time: None,
            hard_delete_scope: None,
            field_family_mode,
        };

        table_def.add_columns_to_maps(column_defs)?;

        table_def.series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                table_def
                    .tag_columns
                    .get_by_id(&id)
                    .expect("tag column should exist by ID")
                    .name()
            })
            .collect::<Vec<Arc<str>>>();
        table_def.series_key = series_key;
        table_def.sort_key = Self::make_sort_key(
            &table_def.series_key_names,
            table_def.timestamp_column.is_some(),
        );

        table_def.rebuild_schema();

        Ok(table_def)
    }

    fn make_sort_key(series_key_names: &[Arc<str>], add_time: bool) -> SortKey {
        let iter = series_key_names.iter().cloned();
        if add_time {
            SortKey::from_columns(iter.chain(iter::once(TIME_COLUMN_NAME.into())))
        } else {
            SortKey::from_columns(iter)
        }
    }

    /// Check if the column name is available and not a reserved name.
    pub(crate) fn check_name(&self, name: impl AsRef<str>) -> Result<()> {
        let name_ref = name.as_ref();

        // Check if it's a reserved column name
        for reserved in RESERVED_COLUMN_NAMES {
            if name_ref == *reserved {
                return Err(CatalogError::ReservedColumn(Arc::from(name_ref)));
            }
        }

        // Check if column already exists
        if let Some(existing) = self.column_definition(name_ref) {
            Err(CatalogError::DuplicateColumn {
                name: existing.name(),
                existing: existing.column_type(),
            })
        } else {
            Ok(())
        }
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl AsRef<str>) -> bool {
        self.columns.contains_name(column.as_ref())
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub(crate) fn add_columns(&mut self, new_columns: Vec<ColumnDefinition>) -> Result<()> {
        self.add_columns_to_maps(new_columns)?;
        self.rebuild_schema();
        Ok(())
    }

    /// Insert columns into the in-memory lookup maps (`columns`, `tag_columns`,
    /// `field_families`, series key, sort key)
    ///
    /// # Note
    ///
    /// This does not rebuild the Arrow schema on the table definition as that would
    /// be costly in a schema-on-write transaction, which adds columns one at a time.
    ///
    /// Therefore, the caller must ensure that `Self::rebuild_schema` is invoked after
    /// all columns have been added via this method.
    pub(crate) fn add_columns_to_maps(&mut self, new_columns: Vec<ColumnDefinition>) -> Result<()> {
        let columns = &mut self.columns;
        let tag_columns = &mut self.tag_columns;
        let mut ffs = HashMap::<FieldFamilyId, FieldFamilyDefinition>::new();

        let mut sort_key_changed = false;

        for col in new_columns {
            let name = col.name();
            assert!(
                !columns.contains_name(&name),
                "attempted to add existing column"
            );

            columns.insert(col.clone())?;

            match col {
                ColumnDefinition::Timestamp(v) => {
                    assert!(
                        self.timestamp_column.is_none(),
                        "table definition initialized with multiple timestamp columns"
                    );
                    self.timestamp_column = Some(v);
                    sort_key_changed = true;
                }
                ColumnDefinition::Tag(v) => {
                    let id = v.id;
                    assert!(
                        tag_columns.insert(v.id, v).is_ok(),
                        "table definition initialized with duplicate tags"
                    );
                    self.series_key.push(id);
                    self.series_key_names.push(name);
                    sort_key_changed = true;
                }
                ColumnDefinition::Field(v) => {
                    // If the field family is not already in the map, insert it
                    if !ffs.contains_key(&v.id.0) {
                        let ff = self
                            .field_families
                            .repo
                            .get(&v.id.0)
                            // This function expects that new field families have already been added
                            .expect("field family ID should exist in the table definition")
                            .as_ref()
                            .clone();
                        ffs.insert(v.id.0, ff);
                    }
                    let ffd = ffs
                        .get_mut(&v.id.0)
                        .expect("field family should exist in the table definition");
                    ffd.fields.insert(v.id.1, v).expect("");
                    self.field_count += 1;
                }
            }
        }

        for (id, ff) in ffs {
            // Replace any updated field family definitions
            self.field_families.repo.insert(id, Arc::new(ff));
        }

        if sort_key_changed {
            self.sort_key = Self::make_sort_key(
                &self.series_key_names,
                self.columns.contains_id(&ColumnIdentifier::Timestamp),
            );
        }

        Ok(())
    }

    /// Rebuild the Arrow `schema` from the current column set.
    fn rebuild_schema(&mut self) {
        // Columns are always sorted by name.
        //
        // We use an unstable sort, as the ColumnSet guarantees no duplicate
        // names, so the order will always be consistent.
        self.columns
            .repo
            .sort_unstable_by(|_, a, _, b| a.name().cmp(&b.name()));

        let mut schema_builder = SchemaBuilder::with_capacity(self.columns.len());
        schema_builder.measurement(self.table_name.as_ref());
        for id in self.columns.repo.keys() {
            let col_def = self
                .columns
                .get_by_id(id)
                .expect("column should exist in the table definition");
            schema_builder.influx_column(col_def.name().as_ref(), col_def.column_type());
        }
        schema_builder.with_series_key(&self.series_key_names);
        self.schema = schema_builder.build().expect("schema should be valid");
    }

    pub fn index_column_ids(&self) -> Vec<TagId> {
        self.tag_columns.repo.keys().cloned().collect()
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_tag_columns(&self) -> usize {
        self.tag_columns.repo.len()
    }

    pub fn num_field_columns(&self) -> usize {
        self.field_count
    }

    pub fn num_field_families(&self) -> usize {
        self.field_families.len()
    }

    /// Returns the field columns for the specified field family, if it exists.
    pub fn fields_by_family(
        &self,
        id: impl Into<FieldFamilyId>,
    ) -> Option<&Repository<FieldId, FieldColumn>> {
        self.field_families
            .get_ref_by_id(&id.into())
            .map(|def| &def.fields)
    }

    pub fn field_family_id_by_field_name(&self, name: impl AsRef<str>) -> Option<FieldFamilyId> {
        self.columns
            .get_by_name(name.as_ref())
            .map(|def| def.as_field().map(|f| f.id.0))?
    }

    pub fn field_type_by_name(&self, name: impl AsRef<str>) -> Option<InfluxColumnType> {
        self.columns
            .get_by_name(name.as_ref())
            .map(|v| v.column_type())
    }

    pub fn column_id_to_name(&self, column_id: &ColumnIdentifier) -> Option<Arc<str>> {
        self.columns.get_by_id(column_id).map(|def| def.name())
    }

    pub fn column_name_to_id(&self, column_name: impl AsRef<str>) -> Option<ColumnIdentifier> {
        self.columns.name_to_id(column_name.as_ref())
    }

    pub fn column_name_to_id_unchecked(&self, column_name: impl AsRef<str>) -> ColumnIdentifier {
        self.columns
            .name_to_id(column_name.as_ref())
            .expect("column ID should exist in the table definition")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<ColumnDefinition> {
        self.columns.get_by_name(name.as_ref()).cloned()
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition_by_id(&self, id: &ColumnIdentifier) -> Option<ColumnDefinition> {
        self.columns.get_by_id(id).cloned()
    }

    pub fn tag_id_by_name(&self, name: impl AsRef<str>) -> Option<TagId> {
        self.tag_columns.name_to_id(name.as_ref())
    }

    pub fn series_key_ids(&self) -> &[TagId] {
        &self.series_key
    }

    pub fn series_key_names(&self) -> &[Arc<str>] {
        &self.series_key_names
    }

    pub(crate) fn auto_field_family(&self) -> Option<FieldFamilyId> {
        self.auto_field_family
    }

    pub(crate) fn next_auto_field_family_name(&self) -> u16 {
        self.next_auto_field_family_name
    }

    pub(crate) fn set_auto_field_family(&mut self, id: FieldFamilyId, name: u16) -> Result<()> {
        // This guard ensures that the auto-name is only set to a value greater than the current.
        if name >= self.next_auto_field_family_name {
            self.next_auto_field_family_name = name
                .checked_add(1)
                .ok_or(CatalogError::TooManyFieldFamilies(u16::MAX as usize + 1))?;
            self.auto_field_family = Some(id);
        }
        Ok(())
    }
}

impl DeletedSchema for TableDefinition {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for TableDefinition {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
    }
}
