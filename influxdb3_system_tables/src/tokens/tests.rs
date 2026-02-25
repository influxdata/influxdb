use super::table_schema;

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
