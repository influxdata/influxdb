use observability_deps::tracing::debug;

use super::*;
// TODO: add a prop test or itertool test combos

#[test]
fn test_actions_read() {
    let read_action = DatabaseActions::new(&["read".to_owned()]).expect("to be parsed");
    assert!(read_action.contains(DatabaseActions::READ));
    assert!(!read_action.contains(DatabaseActions::WRITE));
}

#[test]
fn test_actions_write() {
    let read_action = DatabaseActions::new(&["write".to_owned()]).expect("to be parsed");
    assert!(read_action.contains(DatabaseActions::WRITE));
    assert!(!read_action.contains(DatabaseActions::READ));
}

#[test]
fn test_actions_read_write() {
    let read_action =
        DatabaseActions::new(&["read".to_owned(), "write".to_owned()]).expect("to be parsed");
    assert!(read_action.contains(DatabaseActions::WRITE));
    assert!(read_action.contains(DatabaseActions::READ));
}

#[test]
fn test_actions_describe() {
    let describe_action = DatabaseActions::new(&["describe".to_owned()]).expect("to be parsed");
    assert!(describe_action.contains(DatabaseActions::DESCRIBE));
    assert!(!describe_action.contains(DatabaseActions::READ));
    assert!(!describe_action.contains(DatabaseActions::WRITE));
}

#[test]
fn test_actions_delete() {
    let delete_action = DatabaseActions::new(&["delete".to_owned()]).expect("to be parsed");
    assert!(delete_action.contains(DatabaseActions::DELETE));
    assert!(!delete_action.contains(DatabaseActions::READ));
    assert!(!delete_action.contains(DatabaseActions::WRITE));
}

#[test]
fn test_invalid_database_action() {
    let action = DatabaseActions::new(&["read".to_owned(), "write".to_owned(), "drop".to_owned()])
        .unwrap_err();
    assert!(matches!(
        action,
        ResourceMappingError::ActionNotSupported(_)
    ));
}

#[test]
fn test_invalid_system_action() {
    let action = SystemActions::new(&["read".to_owned(), "write".to_owned(), "create".to_owned()])
        .unwrap_err();
    assert!(matches!(
        action,
        ResourceMappingError::ActionNotSupported(_)
    ));
}

#[test_log::test(test)]
fn test_combine_as_u64_check_keys() {
    let resource_id = DbId::from(5);
    let key = PermissionKey::new(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(resource_id),
    );
    debug!(?key, "key");

    let resource_id = DbId::from(6);
    let key_2 = PermissionKey::new(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(resource_id),
    );
    debug!(?key, "key");

    assert!(key != key_2)
}

#[test_log::test(test)]
fn test_combine_as_u64_check_keys_special_case() {
    let key = PermissionKey::new(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Wildcard,
    );

    let key_2 = PermissionKey::new(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Wildcard,
    );
    assert!(key == key_2);

    let key_3 = PermissionKey::new(
        ResourceType::Token,
        TokenPermissionResourceIdentifier::Wildcard,
    );

    // with different resource types keys must NOT match
    assert!(key != key_3)
}

#[test]
fn test_resource_permission() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["read".to_owned(), "write".to_owned()];
    let allowed_actions = DatabaseActions::new(&actions).expect("to be parsed");
    res_perm.add_permission(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(DbId::from(1)),
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    let perm = res_perm
        .get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(DbId::from(1)),
            &TokenId::from(1),
        )
        .unwrap();

    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::READ));
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::WRITE));
}

#[test]
fn test_resource_permission_read_only() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["read".to_owned()];
    let allowed_actions = DatabaseActions::new(&actions).expect("to be parsed");
    res_perm.add_permission(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(DbId::from(1)),
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    let perm = res_perm
        .get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(DbId::from(1)),
            &TokenId::from(1),
        )
        .unwrap();

    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    // allow read
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::READ));
    assert!(
        DatabaseActions::from(perm.actions)
            .is_allowed(&DatabaseActions::from(DatabaseActions::READ))
    );
    // not allow write
    assert!(!DatabaseActions::from(perm.actions).contains(DatabaseActions::WRITE));
    assert!(
        !DatabaseActions::from(perm.actions)
            .is_allowed(&DatabaseActions::from(DatabaseActions::WRITE))
    );
}

#[test]
fn test_resource_permission_delete_only_does_not_allow_write() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["delete".to_owned()];
    let allowed_actions = DatabaseActions::new(&actions).expect("to be parsed");
    res_perm.add_permission(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(DbId::from(1)),
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    // delete should be allowed
    let delete_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(
            DbId::from(1),
            DatabaseActions::from(DatabaseActions::DELETE),
        ),
    );
    assert_eq!(delete_allowed, Some(true));

    // write should NOT be allowed
    let write_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(DbId::from(1), DatabaseActions::from(DatabaseActions::WRITE)),
    );
    assert_eq!(write_allowed, Some(false));

    // read should NOT be allowed
    let read_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(DbId::from(1), DatabaseActions::from(DatabaseActions::READ)),
    );
    assert_eq!(read_allowed, Some(false));
}

#[test]
fn test_resource_permission_write_only_does_not_allow_delete() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["write".to_owned()];
    let allowed_actions = DatabaseActions::new(&actions).expect("to be parsed");
    res_perm.add_permission(
        ResourceType::Database,
        TokenPermissionResourceIdentifier::Database(DbId::from(1)),
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    // write should be allowed
    let write_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(DbId::from(1), DatabaseActions::from(DatabaseActions::WRITE)),
    );
    assert_eq!(write_allowed, Some(true));

    // delete should NOT be allowed
    let delete_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(
            DbId::from(1),
            DatabaseActions::from(DatabaseActions::DELETE),
        ),
    );
    assert_eq!(delete_allowed, Some(false));

    // read should NOT be allowed
    let read_allowed = res_perm.is_allowed_access(
        &TokenId::from(1),
        AccessRequest::Database(DbId::from(1), DatabaseActions::from(DatabaseActions::READ)),
    );
    assert_eq!(read_allowed, Some(false));
}

#[test]
fn test_resource_permission_read_only_multiple_dbs() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["read".to_owned()];
    let allowed_actions = DatabaseActions::new(&actions).expect("to be parsed");

    for id in 1..=2 {
        res_perm.add_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(DbId::from(id)),
            TokenId::from(1),
            allowed_actions.to_bitmap(),
        );

        // assert token 1 has access to both db 1 and 2
        assert_db_action(&res_perm, allowed_actions, id);
    }
}

#[test]
fn test_resource_permission_for_admin() {
    let mut res_perm = TokenPermissions::new();
    let allowed_actions = Actions::Wildcard;
    res_perm.add_permission(
        ResourceType::Wildcard,
        TokenPermissionResourceIdentifier::Wildcard,
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    // check if admin token has permission for a db action
    let perm = res_perm
        .get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(DbId::from(1)),
            &TokenId::from(1),
        )
        .unwrap();
    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::READ));
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::WRITE));

    // check if admin token has permission for a token action
    let perm = res_perm
        .get_permission(
            ResourceType::Token,
            TokenPermissionResourceIdentifier::Wildcard,
            &TokenId::from(1),
        )
        .unwrap();
    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    assert!(CrudActions::from(perm.actions).contains(CrudActions::READ));
    assert!(CrudActions::from(perm.actions).contains(CrudActions::CREATE));
    assert!(CrudActions::from(perm.actions).contains(CrudActions::UPDATE));
    assert!(CrudActions::from(perm.actions).contains(CrudActions::DELETE));
}

#[test]
fn test_resource_permission_for_system_read_only_access() {
    let mut res_perm = TokenPermissions::new();
    let actions = vec!["read".to_owned()];
    let allowed_actions = SystemActions::new(&actions).expect("to be parsed");
    res_perm.add_permission(
        ResourceType::System,
        TokenPermissionResourceIdentifier::System(SystemResourceIdentifier::from(
            SystemResourceIdentifier::HEALTH,
        )),
        TokenId::from(1),
        allowed_actions.to_bitmap(),
    );

    let perm = res_perm
        .get_permission(
            ResourceType::System,
            TokenPermissionResourceIdentifier::System(SystemResourceIdentifier::from(
                SystemResourceIdentifier::HEALTH,
            )),
            &TokenId::from(1),
        )
        .unwrap();

    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    // allow read
    assert!(SystemActions::from(perm.actions).contains(SystemActions::READ));
    assert!(
        SystemActions::from(perm.actions).is_allowed(&SystemActions::from(SystemActions::READ))
    );
}

fn assert_db_action(res_perm: &TokenPermissions, allowed_actions: DatabaseActions, id: u32) {
    let perm = res_perm
        .get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(DbId::from(id)),
            &TokenId::from(1),
        )
        .unwrap();

    assert_eq!(perm.actions, allowed_actions.to_bitmap());
    // allow read
    assert!(DatabaseActions::from(perm.actions).contains(DatabaseActions::READ));
    assert!(
        DatabaseActions::from(perm.actions)
            .is_allowed(&DatabaseActions::from(DatabaseActions::READ))
    );
    // not allow write
    assert!(!DatabaseActions::from(perm.actions).contains(DatabaseActions::WRITE));
    assert!(
        !DatabaseActions::from(perm.actions)
            .is_allowed(&DatabaseActions::from(DatabaseActions::WRITE))
    );
}

#[test]
fn test_crud_actions_accept_all_four_strings() {
    let parsed = CrudActions::new(&[
        "read".to_owned(),
        "create".to_owned(),
        "update".to_owned(),
        "delete".to_owned(),
    ])
    .expect("parses");
    assert!(parsed.contains(CrudActions::READ));
    assert!(parsed.contains(CrudActions::CREATE));
    assert!(parsed.contains(CrudActions::UPDATE));
    assert!(parsed.contains(CrudActions::DELETE));
}

#[test]
fn token_db_actions_map_to_role_vocabulary() {
    use crate::role::DatabaseAction;

    let read = DatabaseActions::new(&["read".to_string()])
        .unwrap()
        .to_database_actions();
    assert_eq!(read, vec![DatabaseAction::Read]);

    let describe = DatabaseActions::new(&["describe".to_string()])
        .unwrap()
        .to_database_actions();
    assert_eq!(describe, vec![DatabaseAction::Describe]);
}
