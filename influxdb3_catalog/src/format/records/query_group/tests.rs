use std::sync::Arc;

use influxdb3_id::{NodeId, QueryGroupId};
use uuid::Uuid;

use super::{CreateQueryGroup, DeleteQueryGroup, UpdateQueryGroup};
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::resource::CatalogResource;

fn empty_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

fn sample_create() -> CreateQueryGroup {
    CreateQueryGroup {
        query_group_id: 1,
        query_group_name: "analytics".to_string(),
        members: vec![3, 1, 2],
        replication_factor: 2,
    }
}

#[test]
fn record_id_and_flags() {
    assert_eq!(CreateQueryGroup::ID.raw(), 0x8007);
    assert!(!CreateQueryGroup::FLAGS.is_upgrade_safe());

    assert_eq!(UpdateQueryGroup::ID.raw(), 0x8008);
    assert!(!UpdateQueryGroup::FLAGS.is_upgrade_safe());

    assert_eq!(DeleteQueryGroup::ID.raw(), 0x8009);
    assert!(!DeleteQueryGroup::FLAGS.is_upgrade_safe());
}

#[test]
fn create_event_carries_id() {
    match sample_create().event() {
        CatalogEvent::QueryGroupCreated { query_group_id } => {
            assert_eq!(query_group_id, QueryGroupId::new(1));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn update_event_carries_id() {
    let record = UpdateQueryGroup {
        query_group_id: 7,
        query_group_name: "analytics".to_string(),
        members: vec![1],
        replication_factor: 1,
    };
    match record.event() {
        CatalogEvent::QueryGroupUpdated { query_group_id } => {
            assert_eq!(query_group_id, QueryGroupId::new(7));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn delete_event_carries_id() {
    // The event produced by a delete record carries the deleted group's ID.
    match (DeleteQueryGroup { query_group_id: 5 }).event() {
        CatalogEvent::QueryGroupDeleted { query_group_id } => {
            assert_eq!(query_group_id, QueryGroupId::new(5));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn create_round_trip() {
    assert_roundtrip!(sample_create(), "040109616e616c7974696373030406270602");
}

#[test]
fn update_round_trip() {
    assert_roundtrip!(
        UpdateQueryGroup {
            query_group_id: 1,
            query_group_name: "analytics".to_string(),
            members: vec![1, 2, 3],
            replication_factor: 3,
        },
        "040109616e616c7974696373030406390603"
    );
}

#[test]
fn update_round_trip_with_members_out_of_order() {
    assert_roundtrip!(
        UpdateQueryGroup {
            query_group_id: 1,
            query_group_name: "analytics".to_string(),
            members: vec![3, 1, 2],
            replication_factor: 3,
        },
        "040109616e616c7974696373030406270603"
    );
}

#[test]
fn delete_round_trip() {
    assert_roundtrip!(DeleteQueryGroup { query_group_id: 1 }, "0401");
}

#[test]
fn create_apply_inserts_group_preserving_member_order() {
    let mut catalog = empty_catalog();
    sample_create().apply(&mut catalog).expect("apply create");

    let group = catalog
        .query_groups
        .get_by_id(&QueryGroupId::new(1))
        .expect("group present");
    assert_eq!(group.name().as_ref(), "analytics");
    assert_eq!(
        group.members(),
        &[NodeId::new(3), NodeId::new(1), NodeId::new(2)]
    );
    let replication_factor: usize = group.replication_factor().into();
    assert_eq!(replication_factor, 2);
}

#[test]
fn create_apply_rejects_duplicate_id() {
    let mut catalog = empty_catalog();
    sample_create().apply(&mut catalog).expect("first apply");

    let duplicate_id = CreateQueryGroup {
        query_group_id: 1,
        query_group_name: "different-name".to_string(),
        members: vec![4],
        replication_factor: 1,
    };
    let err = duplicate_id.apply(&mut catalog).expect_err("duplicate id");
    assert!(err.0.contains("id 1 already exists"), "{}", err.0);

    // The original group is untouched.
    let group = catalog
        .query_groups
        .get_by_id(&QueryGroupId::new(1))
        .expect("group present");
    assert_eq!(group.name().as_ref(), "analytics");
}

#[test]
fn create_apply_rejects_duplicate_name() {
    let mut catalog = empty_catalog();
    sample_create().apply(&mut catalog).expect("first apply");

    let duplicate_name = CreateQueryGroup {
        query_group_id: 2,
        query_group_name: "analytics".to_string(),
        members: vec![4],
        replication_factor: 1,
    };
    let err = duplicate_name
        .apply(&mut catalog)
        .expect_err("duplicate name");
    assert!(
        err.0.contains("name 'analytics' already exists"),
        "{}",
        err.0
    );
}

#[test]
fn create_apply_rejects_zero_replication_factor() {
    let mut catalog = empty_catalog();
    let record = CreateQueryGroup {
        query_group_id: 1,
        query_group_name: "analytics".to_string(),
        members: vec![1],
        replication_factor: 0,
    };
    let err = record.apply(&mut catalog).expect_err("zero rf");
    assert!(err.0.contains("invalid replication factor 0"), "{}", err.0);
}

#[test]
fn update_apply_replaces_existing_group() {
    let mut catalog = empty_catalog();
    sample_create().apply(&mut catalog).expect("create");

    let update = UpdateQueryGroup {
        query_group_id: 1,
        query_group_name: "analytics".to_string(),
        members: vec![2, 3],
        replication_factor: 1,
    };
    update.apply(&mut catalog).expect("update");

    let group = catalog
        .query_groups
        .get_by_id(&QueryGroupId::new(1))
        .expect("group present");
    assert_eq!(group.members(), &[NodeId::new(2), NodeId::new(3)]);
    let replication_factor: usize = group.replication_factor().into();
    assert_eq!(replication_factor, 1);
}

#[test]
fn update_apply_rejects_missing_group() {
    let mut catalog = empty_catalog();
    let update = UpdateQueryGroup {
        query_group_id: 9,
        query_group_name: "analytics".to_string(),
        members: vec![1],
        replication_factor: 1,
    };
    let err = update.apply(&mut catalog).expect_err("missing group");
    assert!(err.0.contains("id 9 does not exist"), "{}", err.0);
}

#[test]
fn delete_apply_rejects_missing_group() {
    // Deleting a group that was never created is an error, not a no-op.
    let mut catalog = empty_catalog();
    let err = DeleteQueryGroup { query_group_id: 99 }
        .apply(&mut catalog)
        .expect_err("missing group");
    assert!(err.0.contains("id 99 does not exist"), "{}", err.0);
}

#[test]
fn delete_apply_removes_group() {
    // After a successful delete, the group is gone from the repository.
    let mut catalog = empty_catalog();
    sample_create().apply(&mut catalog).expect("create");
    assert!(
        catalog
            .query_groups
            .get_by_id(&QueryGroupId::new(1))
            .is_some(),
        "group should be present before delete"
    );

    DeleteQueryGroup { query_group_id: 1 }
        .apply(&mut catalog)
        .expect("delete");

    assert!(
        catalog
            .query_groups
            .get_by_id(&QueryGroupId::new(1))
            .is_none(),
        "group should be absent after delete"
    );
}
