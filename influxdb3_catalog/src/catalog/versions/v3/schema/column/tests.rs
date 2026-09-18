use super::*;

#[test]
fn insertion_fails_when_resource_exists_by_name_or_id() {
    let definition = ColumnDefinition::Tag(Arc::new(TagColumn {
        column_id: Some(ColumnId::new(0)),
        id: TagId::new(0),
        name: Arc::from("name"),
    }));

    let mut set = ColumnSet::new();
    set.insert(definition.clone()).unwrap();

    set.insert(ColumnDefinition::Timestamp(Arc::new(TimestampColumn {
        column_id: definition.ord_id(),
        name: Arc::from("time"),
    })))
    .unwrap_err();

    set.insert(ColumnDefinition::Tag(Arc::new(TagColumn {
        column_id: Some(ColumnId::new(1)),
        id: TagId::new(0),
        name: Arc::from("tag"),
    })))
    .unwrap_err();

    set.insert(ColumnDefinition::Field(Arc::new(FieldColumn {
        id: FieldIdentifier(FieldFamilyId::new(0), FieldId::new(0)),
        column_id: None,
        name: definition.name(),
        data_type: InfluxFieldType::Boolean,
    })))
    .unwrap_err();

    // and then let's insert something with a None ColumnId and ensure that we can insert other
    // columns with a None column id, since otherwise we wouldn't be able to have more than u16::MAX
    // columns (which is something we explicitly want to support)
    set.insert(ColumnDefinition::Tag(Arc::new(TagColumn {
        column_id: None,
        id: TagId::new(1),
        name: Arc::from("tag1"),
    })))
    .unwrap();

    set.insert(ColumnDefinition::Tag(Arc::new(TagColumn {
        column_id: None,
        id: TagId::new(2),
        name: Arc::from("tag2"),
    })))
    .unwrap();
}
