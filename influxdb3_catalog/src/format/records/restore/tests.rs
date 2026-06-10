use super::RestoreCatalog;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;

#[test]
fn record_id_and_flags() {
    assert_eq!(RestoreCatalog::ID.raw(), 0x8006);
    assert!(!RestoreCatalog::FLAGS.is_upgrade_safe());
}

#[test]
fn event_carries_restore_id() {
    let r = sample();
    match r.event() {
        CatalogEvent::CatalogFullyRestored { restore_id } => {
            assert_eq!(restore_id.as_ref(), "restore-abc");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
#[should_panic(expected = "must be intercepted by the apply driver")]
fn apply_panics() {
    use crate::catalog::versions::v3::inner::InnerCatalog;
    use std::sync::Arc;
    use uuid::Uuid;
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    let _ = sample().apply(&mut catalog);
}

#[test]
fn round_trip() {
    assert_roundtrip!(
        RestoreCatalog {
            time_ns: 1_700_000_000_000_000_000,
            restore_id: "restore-abc".to_string(),
            checkpoint_path: "backups/full-1/catalog/checkpoint".to_string(),
            log_paths: vec![
                "backups/full-1/catalog/logs/00000000000000000001.catalog".to_string(),
                "backups/full-1/catalog/logs/00000000000000000002.catalog".to_string(),
            ],
        },
        "0000002a36fe9c97170b726573746f72652d616263216261636b7570732f66756c6c2d312f636174616c6f672f636865636b706f696e740238386261636b7570732f66756c6c2d312f636174616c6f672f6c6f67732f30303030303030303030303030303030303030312e636174616c6f676261636b7570732f66756c6c2d312f636174616c6f672f6c6f67732f30303030303030303030303030303030303030322e636174616c6f67"
    );
}

fn sample() -> RestoreCatalog {
    RestoreCatalog {
        time_ns: 1_700_000_000_000_000_000,
        restore_id: "restore-abc".to_string(),
        checkpoint_path: "backups/full-1/catalog/checkpoint".to_string(),
        log_paths: vec![],
    }
}
