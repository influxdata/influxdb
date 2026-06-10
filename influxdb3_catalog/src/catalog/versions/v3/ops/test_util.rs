use std::sync::Arc;
use uuid::Uuid;

use influxdb3_id::DbId;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::database::{CreateDatabaseArgs, CreateDatabaseOp};
use crate::format::records::types::{
    ColumnDefinition, FieldColumn, FieldDataType, FieldFamilyDefinition, FieldFamilyMode,
    FieldFamilyName, FieldIdentifier, RetentionPeriod, TagColumn, TimestampColumn,
};
use crate::format::records::{AddColumns, CreateTable};
use crate::format::{REGISTRY, RecordBatch};
use crate::resource::CatalogResource;

pub(crate) fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

pub(crate) fn apply_batch(batch: &RecordBatch, catalog: &mut InnerCatalog) {
    for record in batch.as_slice() {
        let entry = REGISTRY
            .get(record.id())
            .expect("record should be registered");
        (entry.decode_apply_and_event)(&record.data, catalog)
            .expect("test record should apply successfully");
    }
}

pub(crate) fn create_db(catalog: &mut InnerCatalog, name: &str) -> DbId {
    let mut batch = RecordBatch::new(1);
    let op = CreateDatabaseOp::prepare(
        &CreateDatabaseArgs {
            name: name.to_string(),
            retention_period: None,
        },
        catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, catalog);
    op.output(catalog).id()
}

pub(crate) fn create_table(catalog: &mut InnerCatalog, db_name: &str, table_name: &str) {
    let mut batch = RecordBatch::new(1);
    batch.push(&CreateTable {
        database_id: 0,
        database_name: db_name.to_string(),
        table_name: table_name.to_string(),
        table_id: 0,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    });
    batch.push(&AddColumns {
        database_id: 0,
        table_id: 0,
        columns: vec![
            ColumnDefinition::Tag(TagColumn {
                id: 0,
                column_id: Some(0),
                name: "host".to_string(),
            }),
            ColumnDefinition::Field(FieldColumn {
                id: FieldIdentifier {
                    family_id: 0,
                    field_id: 0,
                },
                column_id: Some(1),
                name: "usage".to_string(),
                data_type: FieldDataType::Float,
            }),
            ColumnDefinition::Timestamp(TimestampColumn {
                column_id: Some(2),
                name: "time".to_string(),
            }),
        ],
        field_families: vec![FieldFamilyDefinition {
            id: 0,
            name: FieldFamilyName::Auto(0),
        }],
    });
    apply_batch(&batch, catalog);
}
