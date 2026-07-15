use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::records::RegisterNode;
use crate::format::records::types::NodeMode as WireNodeMode;
use crate::format::{
    CatalogFile, FeatureLevel, FormatError, Header, MakeRecord, RECORD_HEADER_SIZE, Record,
    RecordBatch, RecordFlags, file_flags,
};

use super::{RestorePreload, apply_catalog_file, apply_records, serialize_log_file};

fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

fn log_records(file: &CatalogFile) -> &[Record] {
    assert!(!file.header.is_snapshot(), "expected Log, got Snapshot");
    &file.records
}

fn snapshot_records(file: &CatalogFile) -> &[Record] {
    assert!(file.header.is_snapshot(), "expected Snapshot, got Log");
    &file.records
}

fn sample_register_node() -> RegisterNode {
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![WireNodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
}

#[test]
fn serialize_and_deserialize_round_trip() {
    let record = sample_register_node().make_record(42);
    let records = vec![record];

    let bytes = serialize_log_file(Uuid::nil(), 42, &records);

    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("should parse");

    assert_eq!(file.header.sequence_number, 42);
    assert_eq!(file.header.record_count, 1);
    let log = log_records(&file);
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].id(), records[0].id());
    assert_eq!(log[0].header.sequence, 42);
}

#[test]
fn serialize_multiple_records() {
    let mut batch = RecordBatch::new(10);
    batch.push(&sample_register_node());
    batch.push(&crate::format::records::SetStorageMode {
        mode: crate::format::records::types::StorageMode::PachaTree,
    });

    let bytes = serialize_log_file(Uuid::nil(), 10, batch.as_slice());

    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("should parse");

    assert_eq!(file.header.record_count, 2);
    let log = log_records(&file);
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].header.sequence, 10);
    assert_eq!(log[1].header.sequence, 10);
}

#[test]
fn serialize_empty_batch() {
    let bytes = serialize_log_file(Uuid::nil(), 1, &[]);

    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("should parse");

    assert_eq!(file.header.record_count, 0);
    assert!(log_records(&file).is_empty());
}

#[test]
fn crc_validation_catches_corruption() {
    let record = sample_register_node().make_record(1);
    let bytes = serialize_log_file(Uuid::nil(), 1, &[record]);

    let mut corrupted = bytes.to_vec();
    corrupted[65] ^= 0xFF;

    let mut cursor = Cursor::new(&corrupted);
    let Err(FormatError::Crc32Mismatch { .. }) = CatalogFile::read_from(&mut cursor) else {
        panic!("crc validation should have failed");
    };
}

#[test]
fn apply_records_to_catalog() {
    let mut catalog = test_catalog();
    let record = sample_register_node().make_record(1);
    let records = vec![record];

    let events = apply_records(
        &records,
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], CatalogEvent::NodeRegistered { .. }));
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(1));
    assert!(catalog.nodes.get_by_name("node-a").is_some());
}

#[test]
fn apply_catalog_file_end_to_end() {
    let mut catalog = test_catalog();

    let record = sample_register_node().make_record(5);
    let bytes = serialize_log_file(Uuid::nil(), 5, &[record]);

    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("should parse");

    let events = apply_catalog_file(&file, &mut catalog, &mut RestorePreload::empty()).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(5));
    assert!(catalog.nodes.get_by_name("node-a").is_some());
}

#[test]
fn apply_records_skips_unknown_upgrade_safe_record() {
    let mut catalog = test_catalog();

    // Construct a record with an unregistered ID and UPGRADE_SAFE flag
    let unknown_record = Record::new(
        9999,
        RecordFlags::upgrade_safe(),
        1,
        Bytes::from_static(b""),
    );
    let known_record = sample_register_node().make_record(1);
    let records = vec![unknown_record, known_record];

    let events = apply_records(
        &records,
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    // Only the known record should produce an event
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], CatalogEvent::NodeRegistered { .. }));
    assert!(catalog.nodes.get_by_name("node-a").is_some());
}

#[test]
fn apply_records_errors_on_unknown_non_upgrade_safe_record() {
    let mut catalog = test_catalog();

    let unknown_record = Record::new(9999, RecordFlags::none(), 1, Bytes::from_static(b""));
    let records = vec![unknown_record];

    let result = apply_records(
        &records,
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    );
    assert!(result.is_err());
}

#[test]
fn apply_records_appends_to_ordered_records() {
    let mut catalog = test_catalog();
    assert!(catalog.ordered_records.is_empty());

    let r1 = sample_register_node().make_record(1);
    let r2 = crate::format::records::SetStorageMode {
        mode: crate::format::records::types::StorageMode::PachaTree,
    }
    .make_record(1);

    apply_records(
        &[r1.clone(), r2.clone()],
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    assert_eq!(catalog.ordered_records.len(), 2);
    assert_eq!(catalog.ordered_records[0].id(), r1.id());
    assert_eq!(catalog.ordered_records[1].id(), r2.id());
}

#[test]
fn apply_records_retains_unknown_upgrade_safe_records_for_snapshotting() {
    let mut catalog = test_catalog();

    let unknown = Record::new(
        9999,
        RecordFlags::upgrade_safe(),
        1,
        Bytes::from_static(b""),
    );
    let known = sample_register_node().make_record(1);

    apply_records(
        &[unknown.clone(), known.clone()],
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    // Both records land in ordered_records: the known one was applied, the
    // UPGRADE_SAFE one is preserved verbatim so it survives into the next
    // snapshot written by this node.
    assert_eq!(catalog.ordered_records.len(), 2);
    assert_eq!(catalog.ordered_records[0].id(), unknown.id());
    assert_eq!(catalog.ordered_records[1].id(), known.id());
}

fn sample_create_database(database_id: u32, sequence: u64) -> Record {
    crate::format::records::CreateDatabase {
        database_id,
        database_name: format!("db-{database_id}"),
        retention_period: crate::format::records::types::RetentionPeriod::Indefinite,
    }
    .make_record(sequence)
}

fn sample_create_table(database_id: u32, table_id: u32, sequence: u64) -> Record {
    crate::format::records::CreateTable {
        database_id,
        database_name: format!("db-{database_id}"),
        table_name: format!("table-{table_id}"),
        table_id,
        retention_period: crate::format::records::types::RetentionPeriod::Indefinite,
        field_family_mode: crate::format::records::types::FieldFamilyMode::Auto,
    }
    .make_record(sequence)
}

#[test]
fn serialize_snapshot_empty_catalog_writes_compat_entry_only() {
    // An empty catalog snapshots to a header plus the single
    // backward-compatibility group-index entry: SNAPSHOT flag set, zero
    // records, all-zero entry (Global, zero records, zero bytes).
    let mut catalog = test_catalog();
    let bytes = catalog.create_snapshot();
    let mut cursor = Cursor::new(bytes.as_ref());
    let header = Header::read_from(&mut cursor).expect("valid header");
    assert_eq!(header.flags & file_flags::SNAPSHOT, file_flags::SNAPSHOT);
    assert_eq!(header.record_count, 0);
    assert_eq!(header.group_count, 1);
    assert_eq!(header.payload_len, 24);
    assert_eq!(bytes.len(), Header::SIZE + 24);
    assert_eq!(&bytes.as_ref()[Header::SIZE..], &[0u8; 24]);
}

#[test]
fn create_snapshot_byte_layout_compat_entry_then_flat_records() {
    // Apply records spanning several databases, then inspect the snapshot
    // bytes: one Global group-index entry spanning all records, then the
    // records in application order with original sequences preserved.
    let mut catalog = test_catalog();

    let r_node = sample_register_node().make_record(1);
    let r_db_20 = sample_create_database(20, 2);
    let r_db_10 = sample_create_database(10, 3);
    let r_table_20 = sample_create_table(20, 200, 4);
    let r_table_10 = sample_create_table(10, 100, 5);
    let input = [r_node, r_db_20, r_db_10, r_table_20, r_table_10];

    apply_records(
        &input,
        &mut catalog,
        CatalogSequenceNumber::new(5),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    let bytes = catalog.create_snapshot();
    let mut cursor = Cursor::new(bytes.as_ref());
    let header = Header::read_from(&mut cursor).expect("parse header");

    assert_eq!(header.flags & file_flags::SNAPSHOT, file_flags::SNAPSHOT);
    assert_eq!(header.record_count, 5);
    assert_eq!(header.group_count, 1);

    // payload_len covers the compat index entry plus the concatenated
    // records.
    let records_len: u64 = input
        .iter()
        .map(|r| (RECORD_HEADER_SIZE + r.data.len()) as u64)
        .sum();
    assert_eq!(header.payload_len, 24 + records_len);

    // The compat entry declares one Global group spanning every record.
    let entry = &bytes.as_ref()[Header::SIZE..Header::SIZE + 24];
    assert_eq!(u32::from_le_bytes(entry[0..4].try_into().unwrap()), 0); // group_type Global
    assert_eq!(u32::from_le_bytes(entry[4..8].try_into().unwrap()), 0); // group_key
    assert_eq!(u32::from_le_bytes(entry[8..12].try_into().unwrap()), 5); // record_count
    assert_eq!(
        u32::from_le_bytes(entry[12..16].try_into().unwrap()) as u64,
        records_len,
    );
    assert_eq!(u64::from_le_bytes(entry[16..24].try_into().unwrap()), 0); // byte_offset

    // The first record's header begins right after the entry, verbatim.
    let first = &bytes.as_ref()[Header::SIZE + 24..Header::SIZE + 24 + RECORD_HEADER_SIZE];
    assert_eq!(
        u16::from_le_bytes(first[0..2].try_into().unwrap()),
        input[0].id().raw(),
    );
    assert_eq!(
        u64::from_le_bytes(first[4..12].try_into().unwrap()),
        input[0].header.sequence,
    );
}

#[test]
fn snapshot_round_trip_preserves_application_order_and_sequences() {
    // End-to-end regression pin for #4019: build a multi-database catalog
    // with records applied at different sequences, snapshot, read back, and
    // verify records are in EXACT original application order with their
    // per-record sequences preserved.
    let mut catalog = test_catalog();

    let input = [
        sample_register_node().make_record(1),
        sample_create_database(20, 2),
        sample_create_database(10, 3),
        sample_create_table(20, 200, 4),
        sample_create_table(10, 100, 5),
        sample_create_table(10, 101, 6),
    ];
    apply_records(
        &input,
        &mut catalog,
        CatalogSequenceNumber::new(6),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    let bytes = catalog.create_snapshot();
    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("reader parses snapshot");

    let parsed: Vec<(_, u64)> = snapshot_records(&file)
        .iter()
        .map(|r| (r.id(), r.header.sequence))
        .collect();
    let expected: Vec<(_, u64)> = input.iter().map(|r| (r.id(), r.header.sequence)).collect();
    assert_eq!(parsed, expected);
}

#[test]
fn snapshot_round_trip_replays_into_fresh_catalog() {
    // Building on the grouping test above: snapshot the source catalog,
    // apply the parsed snapshot to a fresh catalog, and confirm the
    // replay yields the same `ordered_records` set and sequence number.
    let mut source = test_catalog();
    let records = [
        sample_register_node().make_record(1),
        sample_create_database(10, 2),
        sample_create_database(20, 3),
        sample_create_table(10, 100, 4),
        sample_create_table(20, 200, 5),
    ];
    apply_records(
        &records,
        &mut source,
        CatalogSequenceNumber::new(5),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    let bytes = source.create_snapshot();
    let mut cursor = Cursor::new(bytes.as_ref());
    let file = CatalogFile::read_from(&mut cursor).expect("parse snapshot");

    let mut replay = test_catalog();
    let events = apply_catalog_file(&file, &mut replay, &mut RestorePreload::empty()).unwrap();
    assert_eq!(events.len(), records.len());
    assert_eq!(replay.sequence_number(), source.sequence_number());
    assert_eq!(replay.ordered_records.len(), source.ordered_records.len());
}

#[test]
fn apply_records_failure_retains_pre_failure_records() {
    // apply_records is not atomic: if a record mid-batch fails to apply,
    // the catalog is left in a partial state and earlier records have
    // already been pushed into ordered_records. A snapshot taken after
    // such a failure must still serialize and round-trip the pre-failure
    // records cleanly.
    let mut catalog = test_catalog();

    let r1 = sample_register_node().make_record(1);
    let r2_conflicting = RegisterNode {
        node_catalog_id: 2,
        node_id: "node-a".to_string(),
        instance_id: "inst-2".to_string(),
        ..sample_register_node()
    }
    .make_record(1);
    let r3 = crate::format::records::SetStorageMode {
        mode: crate::format::records::types::StorageMode::PachaTree,
    }
    .make_record(1);

    let result = apply_records(
        &[r1.clone(), r2_conflicting, r3],
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    );
    assert!(result.is_err());

    assert_eq!(catalog.ordered_records.len(), 1);
    assert_eq!(catalog.ordered_records[0].id(), r1.id());

    let snapshot = catalog.create_snapshot();
    let mut cursor = Cursor::new(snapshot.as_ref());
    let parsed = CatalogFile::read_from(&mut cursor).expect("should parse");
    let records = snapshot_records(&parsed);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), r1.id());
}

#[cfg(feature = "true_deletion")]
#[test]
fn apply_records_removes_hard_deleted_data() {
    use crate::format::records::{
        CreateTrigger, HardDeleteDatabase, HardDeleteTable, NextIdScope, SetNextId,
        types::{NodeSpec, TriggerSettings, TriggerSpec},
    };

    let mut catalog = test_catalog();

    let records = [
        sample_register_node().make_record(1),
        sample_create_database(1, 2),
        sample_create_table(1, 1, 3),
        sample_create_table(1, 2, 4),
        sample_create_database(2, 5),
        sample_create_table(2, 1, 6),
        CreateTrigger {
            trigger_id: 1,
            trigger_name: "trigger1".to_string(),
            plugin_filename: "plugin_filename".to_string(),
            database_id: 1,
            node_spec: NodeSpec::default(),
            trigger: TriggerSpec::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: None,
            disabled: true,
        }
        .make_record(7),
        HardDeleteDatabase { db_id: 1 }.make_record(8),
        HardDeleteTable {
            db_id: 2,
            table_id: 1,
        }
        .make_record(9),
    ];

    apply_records(
        &records,
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .unwrap();

    let snapshot = catalog.create_snapshot();
    let mut cursor = Cursor::new(snapshot.as_ref());
    let parsed = CatalogFile::read_from(&mut cursor).unwrap();
    let records = snapshot_records(&parsed);

    let expected_records = [
        sample_register_node().make_record(1),
        SetNextId {
            id: 1,
            scope: NextIdScope::Databases,
        }
        .make_record(2),
        sample_create_database(2, 5),
        SetNextId {
            id: 1,
            scope: NextIdScope::Tables { database_id: 2 },
        }
        .make_record(6),
    ];

    assert_eq!(&records, &expected_records);
}
