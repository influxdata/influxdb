use super::*;

#[test]
fn record_header_round_trip() {
    let header = RecordHeader::new(42, RecordFlags::none(), 9999, 11);

    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), RECORD_HEADER_SIZE);
    let mut cursor = Cursor::new(bytes);

    let parsed = RecordHeader::read_from(&mut cursor).unwrap();
    assert_eq!(parsed.id.raw(), 42);
    assert!(!parsed.flags.is_upgrade_safe());
    assert_eq!(parsed.sequence, 9999);
    assert_eq!(parsed.length, 11);
}

#[test]
fn record_round_trip() {
    let data = Bytes::from_static(b"test data for record");
    let record = Record::new(100, RecordFlags::default(), 42, data.clone());

    let bytes = record.to_bytes();
    let buf = Bytes::from(bytes);
    let mut cursor = Cursor::new(buf);

    let parsed = Record::read_from(&mut cursor).unwrap();
    assert_eq!(cursor.position() as usize, RECORD_HEADER_SIZE + data.len());
    assert_eq!(parsed.id().raw(), 100);
    assert_eq!(parsed.sequence(), 42);
    assert!(!parsed.is_upgrade_safe());
    assert_eq!(parsed.data, data);
}

#[test]
fn record_header_buffer_too_short() {
    let bytes = [0u8; 5];
    let mut cursor = Cursor::new(bytes);
    let result = RecordHeader::read_from(&mut cursor);

    assert!(matches!(
        result,
        Err(FormatError::BufferTooShort {
            expected: 16,
            actual: 5
        })
    ));
}

#[test]
fn record_data_buffer_too_short() {
    let header = RecordHeader::new(1, RecordFlags::default(), 0, 100);

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(b"short");

    let buf = Bytes::from(buf);
    let mut cursor = Cursor::new(buf);
    let result = Record::read_from(&mut cursor);

    assert!(matches!(result, Err(FormatError::BufferTooShort { .. })));
}

#[test]
fn multiple_records_parsing() {
    let record1 = Record::new(1, RecordFlags::default(), 10, Bytes::from_static(b"first"));
    let record2 = Record::new(
        2,
        RecordFlags::upgrade_safe(),
        11,
        Bytes::from_static(b"second record"),
    );

    let mut buf = Vec::new();
    buf.extend_from_slice(&record1.to_bytes());
    buf.extend_from_slice(&record2.to_bytes());

    let buf = Bytes::from(buf);
    let mut cursor = Cursor::new(buf);

    let parsed1 = Record::read_from(&mut cursor).unwrap();
    assert_eq!(parsed1.id().raw(), 1);
    assert_eq!(parsed1.sequence(), 10);
    assert_eq!(parsed1.data, Bytes::from_static(b"first"));

    let parsed2 = Record::read_from(&mut cursor).unwrap();
    assert_eq!(parsed2.id().raw(), 2);
    assert_eq!(parsed2.sequence(), 11);
    assert!(parsed2.is_upgrade_safe());
    assert_eq!(parsed2.data, Bytes::from_static(b"second record"));
}

#[test]
fn record_header_byte_layout() {
    let header = RecordHeader::new(
        0x0102,
        RecordFlags::upgrade_safe(),
        0x0304050607080910,
        0x11121314,
    );

    let bytes = header.to_bytes();

    // id (little-endian)
    assert_eq!(&bytes[0x00..0x02], &[0x02, 0x01]);
    // flags (little-endian, UPGRADE_SAFE = 0x0001)
    assert_eq!(&bytes[0x02..0x04], &[0x01, 0x00]);
    // sequence (little-endian)
    assert_eq!(
        &bytes[0x04..0x0C],
        &[0x10, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03]
    );
    // length (little-endian)
    assert_eq!(&bytes[0x0C..0x10], &[0x14, 0x13, 0x12, 0x11]);
}

#[test]
fn record_batch_stamps_pushed_records_with_sequence() {
    use crate::format::records::SetGenerationDuration;

    let mut batch = RecordBatch::new(42);
    batch.push(&SetGenerationDuration {
        level: 1,
        duration_ns: 1_000_000,
    });
    batch.push(&SetGenerationDuration {
        level: 2,
        duration_ns: 2_000_000,
    });

    assert_eq!(batch.sequence(), 42);
    assert_eq!(batch.len(), 2);
    for record in batch.as_slice() {
        assert_eq!(record.sequence(), 42);
    }
}

#[cfg(feature = "true_deletion")]
#[test]
fn record_batch_correctly_removes_db_records() {
    use crate::format::records::{
        AddColumns, ClearDbRetentionPeriod, CreateDatabase, CreateTable, CreateTrigger,
        DeleteTrigger, DisableTrigger, EnableTrigger, HardDeleteDatabase, HardDeleteTable,
        NextIdScope, SetDbRetentionPeriod, SetNextId,
        types::{
            ColumnDefinition, ErrorBehavior, FieldFamilyDefinition, FieldFamilyMode,
            FieldFamilyName, NodeSpec, RetentionPeriod, TagColumn, TimestampColumn,
            TriggerSettings, TriggerSpec,
        },
    };
    use influxdb3_id::DbId;
    use std::collections::BTreeSet;

    let database_id: u32 = 0;
    fn db_name() -> String {
        "db0".to_string()
    }

    let table_id: u32 = 0;
    fn table_name() -> String {
        "table0".to_string()
    }

    let trigger_id: u32 = 0;
    fn trigger_name() -> String {
        "trigger0".to_string()
    }

    let nondeleted_db_id: u32 = 1;

    let mut batch = RecordBatch::new(0);
    batch.push(&CreateDatabase {
        database_id,
        database_name: db_name(),
        retention_period: RetentionPeriod::Indefinite,
    });

    batch.push(&CreateTable {
        database_id,
        database_name: db_name(),
        table_id,
        table_name: table_name(),
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Aware,
    });

    batch.push(&AddColumns {
        database_id,
        table_id,
        columns: vec![
            ColumnDefinition::Timestamp(TimestampColumn {
                column_id: Some(0),
                name: "time".to_string(),
            }),
            ColumnDefinition::Tag(TagColumn {
                id: 1,
                column_id: Some(1),
                name: "tag1".to_string(),
            }),
        ],
        field_families: vec![FieldFamilyDefinition {
            id: 2,
            name: FieldFamilyName::Auto(2),
        }],
    });

    batch.push(&HardDeleteTable {
        db_id: database_id,
        table_id,
    });

    // let's put these trigger ones out-of-order just to make sure there's no logic dependent upon
    // finding a creation event first or smth like that.
    batch.push(&DisableTrigger {
        db_id: database_id,
        trigger_id,
        trigger_name: trigger_name(),
    });

    batch.push(&EnableTrigger {
        db_id: database_id,
        trigger_id,
        trigger_name: trigger_name(),
    });

    batch.push(&DeleteTrigger {
        trigger_id,
        trigger_name: trigger_name(),
        database_id,
        force: true,
    });

    batch.push(&CreateTrigger {
        trigger_id,
        trigger_name: trigger_name(),
        plugin_filename: "plugin.whatever".into(),
        database_id,
        node_spec: NodeSpec::All,
        trigger: TriggerSpec::AllTablesWalWrite,
        trigger_settings: TriggerSettings {
            run_async: false,
            error_behavior: ErrorBehavior::Log,
        },
        trigger_arguments: None,
        disabled: false,
    });

    batch.push(&SetDbRetentionPeriod {
        database_id,
        retention_period: RetentionPeriod::Indefinite,
    });

    batch.push(&ClearDbRetentionPeriod { database_id });

    // ok this is enough, even though there are more variants that we try to clear when running this.

    batch.push(&CreateDatabase {
        database_id: nondeleted_db_id,
        database_name: "db1".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    });

    assert_eq!(batch.len(), 11);

    batch.push(&HardDeleteDatabase { db_id: database_id });

    batch.push(&SetNextId {
        scope: NextIdScope::Databases,
        id: 2,
    });

    batch.push(&SetNextId {
        scope: NextIdScope::Tables { database_id },
        id: 1,
    });

    batch.push(&SetNextId {
        scope: NextIdScope::Tables {
            database_id: nondeleted_db_id,
        },
        id: 1,
    });

    // Make sure that pushing a hard delete or SetNextId doesn't immediately clear the batch of
    // everything related to it - pushing should be a clear and simple operation.
    assert_eq!(batch.len(), 15);

    hard_delete_records_for(
        &mut batch.records,
        &BTreeSet::from_iter([DbId::new(database_id)]),
        &BTreeSet::new(),
    )
    .unwrap();

    // there should only be 4 left:
    // 1. The database creation for the db with id 1,
    // 2. The database hard delete (since we need to transform that into a `SetNextId` - it
    //    shouldn't be removed.)
    // 3. The `SetNextId` for databases
    // 4. The `SetNextId` for tables within a database which wasn't deleted
    assert_eq!(batch.len(), 4);
}

#[cfg(feature = "true_deletion")]
#[test]
fn record_removal_is_best_effort() {
    use crate::format::{
        record_ids,
        records::{
            AckStopNode, CreateAdminToken, CreateDatabase, CreateTable, SetDbRetentionPeriod,
            SoftDeleteDatabase,
            types::{FieldFamilyMode, RetentionPeriod},
        },
    };
    use influxdb3_id::DbId;
    use std::collections::BTreeSet;

    let mut batch = RecordBatch::new(0);

    // normal record which we don't expect to be removed
    batch.push(&CreateAdminToken {
        token_id: 0,
        name: "token0".into(),
        hash: Vec::new(),
        created_at: 0,
        updated_at: None,
        expiry: None,
        description: None,
        created_by: None,
        updated_by: None,
    });

    // a token which could never have data relevant to specific databases which we might need to
    // remove. We mess with its data to ensure that we don't get an error when trying to find
    // records to remove because we shouldn't even look at it at all due to it not having an id that
    // indicates it could contain database-specific info
    batch.push(&AckStopNode {
        node_catalog_id: 0,
        node_id: String::new(),
        ack_time_ns: 0,
        process_uuid: [0; 16],
        final_snapshot_sequence: None,
    });
    batch.records.last_mut().unwrap().data = Bytes::new();

    // shouldn't encounter any errors 'cause neither of them could contain database-specific info so
    // the one that has corrupted data shouldn't even be read
    hard_delete_records_for(
        &mut batch.records,
        &BTreeSet::from_iter([DbId::new(0)]),
        &BTreeSet::new(),
    )
    .unwrap();

    // We shouldn't have removed anything 'cause they were both irrelevant to databases
    assert_eq!(batch.len(), 2);

    let database_id: u32 = 0;
    batch.push(&CreateDatabase {
        database_id,
        database_name: "db0".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    });

    batch.push(&SetDbRetentionPeriod {
        database_id,
        retention_period: RetentionPeriod::Indefinite,
    });
    batch.records.last_mut().unwrap().data = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);

    batch.push(&CreateTable {
        database_id,
        database_name: "db1".to_string(),
        table_id: 0,
        table_name: "table0".to_string(),
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Aware,
    });
    batch.records.last_mut().unwrap().data = Bytes::new();

    batch.push(&SoftDeleteDatabase {
        database_id,
        deletion_time_ns: 0,
        hard_deletion_time_ns: None,
        hard_delete_scope: None,
    });

    let err = hard_delete_records_for(
        &mut batch.records,
        &BTreeSet::from_iter([DbId::new(database_id)]),
        &BTreeSet::new(),
    )
    .unwrap_err();

    // We should've only removed one record - the `SoftDeleteDatabase` - since it's the only one
    // that is readable and relevant to this database and not the creation event.
    assert_eq!(batch.len(), 5);

    // The creation event should've been transformed into a `SetNextId`.
    assert_eq!(batch.records[2].id(), record_ids::SET_NEXT_ID);

    // And we need to make sure that the most recent error is the one that is returned. Not for any
    // particular reason, I guess, I'd just like to make sure that we're aware if that behavior changes.
    assert_eq!(err.to_string(), "invalid record length: 0");
}
