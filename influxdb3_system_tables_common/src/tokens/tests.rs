use std::sync::Arc;

use arrow::array::AsArray;
use influxdb3_authz::TokenInfo;
use influxdb3_catalog::catalog::Catalog;
use iox_system_tables::IoxSystemTable;

use super::{TokenPermissionsFormatter, TokenSystemTable, table_schema};

#[test]
fn test_schema_with_auth() {
    let schema = table_schema(true);
    // should have at least one field with name "hash"
    assert_eq!(schema.fields().iter().len(), 10);
    assert!(schema.fields.iter().any(|field| field.name() == "hash"));
}

#[test]
fn test_schema_without_auth() {
    let schema = table_schema(false);
    // no field should have name "hash"
    assert_eq!(schema.fields().iter().len(), 9);
    schema
        .fields
        .iter()
        .for_each(|field| assert!(field.name() != "hash"));
}

#[derive(Debug)]
struct AllPermissionsFormatter;

impl TokenPermissionsFormatter for AllPermissionsFormatter {
    fn format_permissions(&self, tokens: &[Arc<TokenInfo>]) -> Vec<String> {
        tokens.iter().map(|_| "*:*:*".to_string()).collect()
    }
}

async fn sorted_token_names(table: &TokenSystemTable) -> Vec<String> {
    let batch = table.scan(None, None).await.unwrap();
    let mut names: Vec<String> = batch
        .column_by_name("name")
        .unwrap()
        .as_string_view()
        .iter()
        .map(|name| name.unwrap().to_string())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn test_operator_token_hidden() {
    let catalog = Catalog::new_in_memory("test").await.unwrap();
    catalog.create_admin_token(false).await.unwrap();
    catalog
        .create_named_admin_token("customer_admin".to_string(), None)
        .await
        .unwrap();

    let table = TokenSystemTable::new(
        Arc::clone(&catalog),
        true,
        Arc::new(AllPermissionsFormatter),
    );
    assert_eq!(
        sorted_token_names(&table).await,
        ["_admin", "customer_admin"]
    );

    let table = TokenSystemTable::new(catalog, true, Arc::new(AllPermissionsFormatter))
        .with_operator_token_hidden(true);
    assert_eq!(sorted_token_names(&table).await, ["customer_admin"]);
}
