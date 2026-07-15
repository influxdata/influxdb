use super::parse_qualified_field_name;

#[test]
fn parses_qualified_and_unqualified_names() {
    assert_eq!(parse_qualified_field_name("cpu::user"), Some("cpu"));
    assert_eq!(parse_qualified_field_name("a::b::c"), Some("a"));
    assert_eq!(parse_qualified_field_name("bare"), None);
    assert_eq!(parse_qualified_field_name("::field"), None);
    assert_eq!(parse_qualified_field_name("family::"), None);
    assert_eq!(parse_qualified_field_name("::"), None);
    assert_eq!(parse_qualified_field_name(""), None);
}

use std::sync::Arc;

use influxdb3_id::{ColumnId, TableId};

use super::{StorageMode, TableTransaction};
use crate::CatalogError;
use crate::catalog::versions::v3::schema::column::FieldFamilyMode;
use crate::catalog::versions::v3::schema::table::TableDefinition;

fn table_tx(next_column_id: Option<ColumnId>, storage_mode: StorageMode) -> TableTransaction {
    let mut table =
        TableDefinition::new_empty(TableId::new(1), Arc::from("t"), FieldFamilyMode::Auto);
    table.columns.next_id = next_column_id;
    TableTransaction::from_existing(table, 1_000_000, 1_000_000, storage_mode)
}

#[test]
fn legacy_column_id_available_when_space_remains() {
    let tx = table_tx(Some(ColumnId::new(7)), StorageMode::PachaTree);
    assert_eq!(tx.next_legacy_column_id().unwrap(), Some(ColumnId::new(7)));
    let tx = table_tx(Some(ColumnId::new(7)), StorageMode::Parquet);
    assert_eq!(tx.next_legacy_column_id().unwrap(), Some(ColumnId::new(7)));
}

#[test]
fn legacy_column_id_exhausted_pachatree_yields_none() {
    let tx = table_tx(None, StorageMode::PachaTree);
    assert_eq!(tx.next_legacy_column_id().unwrap(), None);
}

#[test]
fn legacy_column_id_exhausted_parquet_errors() {
    for mode in [StorageMode::Parquet, StorageMode::ParquetAndPachaTree] {
        let tx = table_tx(None, mode);
        assert!(matches!(
            tx.next_legacy_column_id(),
            Err(CatalogError::LegacyColumnIdsExhausted { .. })
        ));
    }
}
