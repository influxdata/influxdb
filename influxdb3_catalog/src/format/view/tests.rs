use super::*;
use crate::format::apply::serialize_log_file;
use crate::format::records::CreateDatabase;
use crate::format::records::types::RetentionPeriod;
use crate::format::{MakeRecord, RecordFlags};
use std::io::Cursor;
use uuid::Uuid;

fn create_database_record(seq: u64, id: u32, name: &str) -> Record {
    CreateDatabase {
        database_id: id,
        database_name: name.to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .make_record(seq)
}

#[test]
fn log_view_decodes_records() {
    let records = vec![create_database_record(1, 1, "alpha")];
    let bytes = serialize_log_file(Uuid::nil(), 1, &records);
    let file = CatalogFile::read_from(&mut Cursor::new(bytes.as_ref())).unwrap();

    let header = header_view(&file);
    assert!(!header.snapshot);
    assert_eq!(header.record_count, 1);

    let views = record_views(&file);
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].header.name, Some("CreateDatabase"));
    match &views[0].body {
        RecordBodyView::Decoded(v) => {
            assert_eq!(v["database_name"], serde_json::json!("alpha"))
        }
        other => panic!("expected decoded body, got {other:?}"),
    }
}

#[test]
fn unknown_record_id_renders_hex_not_panic() {
    let record = Record::new(
        0x7ABC,
        RecordFlags::none(),
        1,
        bytes::Bytes::from_static(&[1, 2, 3]),
    );
    let view = RecordView::from_record(&record);
    assert_eq!(view.header.name, None);
    match view.body {
        RecordBodyView::Unknown { unknown, hex } => {
            assert!(unknown);
            assert_eq!(hex, "010203");
        }
        other => panic!("expected unknown body, got {other:?}"),
    }
}
