use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::CatalogError;
use crate::catalog::versions::v3::catalog::Catalog;
use crate::catalog::{QueryGroupInsertPosition, QueryGroupUpdate};
use crate::resource::CatalogResource;
use influxdb3_id::NodeId;

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

// Open an in-memory catalog and extract the inner Catalog.
async fn new_catalog() -> Arc<Catalog> {
    Catalog::new_in_memory("test").await.expect("catalog")
}

// Build a NonZeroUsize replication factor from a non-zero usize.
fn rf(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("non-zero")
}

fn assert_not_found<T: fmt::Debug>(result: Result<T, CatalogError>) {
    assert!(
        matches!(result, Err(CatalogError::NotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

fn assert_invalid_configuration<T: fmt::Debug>(result: Result<T, CatalogError>) {
    assert!(
        matches!(result, Err(CatalogError::InvalidConfiguration { .. })),
        "expected InvalidConfiguration, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Test creating a query group whose name is already taken.
#[tokio::test]
async fn create_query_group_op_duplicate_name() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    catalog
        .create_query_group("g1", vec![nodes[0], nodes[1]], rf(1))
        .await
        .expect("first create");
    let result = catalog
        .create_query_group("g1", vec![nodes[2]], rf(1))
        .await;
    assert!(
        matches!(result, Err(CatalogError::AlreadyExists)),
        "expected AlreadyExists, got {result:?}"
    );
}

// Test creating a query group with a happy path.
#[tokio::test]
async fn create_query_group_op_success() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    let group = catalog
        .create_query_group("analytics", nodes.clone(), rf(2))
        .await
        .expect("create");

    assert_eq!(group.name().as_ref(), "analytics");
    assert_eq!(group.members(), nodes.as_slice());
    assert_eq!(group.replication_factor(), rf(2));

    // Verify it is retrievable by name.
    let by_name = catalog
        .query_group_by_name("analytics")
        .expect("get by name");
    assert_eq!(by_name.id(), group.id());

    // Verify it is retrievable by ID.
    let by_id = catalog.query_group_by_id(&group.id()).expect("get by id");
    assert_eq!(by_id.id(), group.id());
}

// Test creating a query group with a member order preserved.
#[tokio::test]
async fn create_query_group_op_member_order_preserved() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    // Use a non-sequential order to verify the catalog preserves it.
    let members = vec![nodes[2], nodes[0], nodes[1]];
    let group = catalog
        .create_query_group("ordered", members.clone(), rf(1))
        .await
        .expect("create");

    assert_eq!(
        group.members(),
        members.as_slice(),
        "member order must be preserved exactly"
    );
}

#[tokio::test]
async fn list_query_groups_empty() {
    let catalog = new_catalog().await;
    assert!(catalog.list_query_groups().is_empty(), "expected no groups");
}

#[tokio::test]
async fn list_query_groups_returns_all() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(2).await;
    catalog
        .create_query_group("a", vec![nodes[0]], rf(1))
        .await
        .expect("create a");
    catalog
        .create_query_group("b", vec![nodes[1]], rf(1))
        .await
        .expect("create b");

    let groups = catalog.list_query_groups();
    assert_eq!(groups.len(), 2);
    let names: Vec<_> = groups.iter().map(|g| g.name().to_string()).collect();
    assert!(names.contains(&"a".to_string()));
    assert!(names.contains(&"b".to_string()));
}

#[tokio::test]
async fn query_group_by_name_not_found() {
    let catalog = new_catalog().await;
    assert!(
        catalog.query_group_by_name("nonexistent").is_none(),
        "expected None"
    );
}

#[tokio::test]
async fn query_group_by_id_not_found() {
    use influxdb3_id::QueryGroupId;
    let catalog = new_catalog().await;
    assert!(
        catalog.query_group_by_id(&QueryGroupId::new(999)).is_none(),
        "expected None"
    );
}

#[tokio::test]
async fn add_query_group_member_appends() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(2).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0]], rf(1))
        .await
        .expect("create");

    let updated = catalog
        .add_query_group_member(&group.id(), nodes[1], QueryGroupInsertPosition::Append)
        .await
        .expect("append member");

    assert_eq!(
        updated.members(),
        &[nodes[0], nodes[1]],
        "member should be appended"
    );
    assert_eq!(updated.name().as_ref(), "g1");
    assert_eq!(updated.replication_factor(), rf(1));
}

#[tokio::test]
async fn add_query_group_member_inserts_at_index() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0], nodes[2]], rf(1))
        .await
        .expect("create");

    let updated = catalog
        .add_query_group_member(&group.id(), nodes[1], QueryGroupInsertPosition::Index(1))
        .await
        .expect("insert member");

    assert_eq!(
        updated.members(),
        &[nodes[0], nodes[1], nodes[2]],
        "member should be inserted between nodes[0] and nodes[2]"
    );
}

#[tokio::test]
async fn remove_query_group_member_persists_remaining_members() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0], nodes[1], nodes[2]], rf(1))
        .await
        .expect("create");
    assert_eq!(
        group.members(),
        &[nodes[0], nodes[1], nodes[2]],
        "check that group has 3 members"
    );

    let updated = catalog
        .remove_query_group_member(&group.id(), nodes[1])
        .await
        .expect("remove member");

    assert_eq!(
        updated.members(),
        &[nodes[0], nodes[2]],
        "middle member should be removed"
    );
    assert_eq!(updated.name().as_ref(), "g1");
    assert_eq!(updated.replication_factor(), rf(1));
}

#[tokio::test]
async fn replace_query_group_members_preserves_order() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(3).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0], nodes[1], nodes[2]], rf(1))
        .await
        .expect("create");
    assert_eq!(
        group.members(),
        &[nodes[0], nodes[1], nodes[2]],
        "check initial member order"
    );

    let updated = catalog
        .replace_query_group_members(&group.id(), vec![nodes[2], nodes[0]])
        .await
        .expect("replace members");

    assert_eq!(
        updated.members(),
        &[nodes[2], nodes[0]],
        "members should be replaced and new order preserved"
    );
}

#[tokio::test]
async fn update_query_group_with_members_preserves_omitted_fields() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(2).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0]], rf(2))
        .await
        .expect("create");
    assert_eq!(
        group.members(),
        &[nodes[0]],
        "check that group has 1 member"
    );
    assert_eq!(
        group.replication_factor(),
        rf(2),
        "check that group has RF=2"
    );

    let updated = catalog
        .update_query_group(
            &group.id(),
            QueryGroupUpdate {
                name: None,
                members: Some(vec![nodes[0], nodes[1]]),
                replication_factor: None,
            },
        )
        .await
        .expect("update members");

    assert_eq!(updated.name().as_ref(), "g1");
    assert_eq!(
        updated.members(),
        &[nodes[0], nodes[1]],
        "member list should be updated"
    );
    assert_eq!(
        updated.replication_factor(),
        rf(2),
        "replication factor should keep the same since it was omitted in the update"
    );
}

// Verify that an RF update persists the new RF while leaving name and member order
// unchanged, and that the stored definition reflects the change immediately.
#[tokio::test]
async fn update_query_group_replication_factor_persists_full_state() {
    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(2).await;
    // Use reversed order to confirm member order is preserved exactly.
    let group = catalog
        .create_query_group("analytics", vec![nodes[1], nodes[0]], rf(1))
        .await
        .expect("create");

    let updated = catalog
        .update_query_group_replication_factor(&group.id(), rf(2))
        .await
        .expect("update RF");

    assert_eq!(updated.replication_factor(), rf(2));
    assert_eq!(updated.name().as_ref(), "analytics", "name unchanged");
    assert_eq!(
        updated.members(),
        &[nodes[1], nodes[0]],
        "member order unchanged"
    );

    // Confirm the stored definition reflects the new RF.
    let stored = catalog
        .query_group_by_id(&group.id())
        .expect("group present after RF update");
    assert_eq!(stored.replication_factor(), rf(2));
}

#[tokio::test]
async fn query_group_member_mutation_rejects_invalid_targets() {
    use influxdb3_id::QueryGroupId;

    let catalog = new_catalog().await;
    let nodes = catalog.register_query_nodes(1).await;
    let group = catalog
        .create_query_group("g1", vec![nodes[0]], rf(1))
        .await
        .expect("create");

    assert_not_found(
        catalog
            .add_query_group_member(
                &QueryGroupId::new(999), // non-existent group
                nodes[0],
                QueryGroupInsertPosition::Append,
            )
            .await,
    );
    assert_invalid_configuration(
        catalog
            .add_query_group_member(
                &group.id(),
                nodes[0],
                QueryGroupInsertPosition::Index(99), // invalid index (only 1 member exists)
            )
            .await,
    );
    assert_not_found(
        catalog
            .remove_query_group_member(&group.id(), NodeId::new(999)) // non-existent member
            .await,
    );
}
