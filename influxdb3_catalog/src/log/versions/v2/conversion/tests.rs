use crate::log::versions::v2::enterprise::PermissionDetails;

use super::append_create_if_v2_write_action;

#[test]
fn test_append_token_with_create_action_for_v2_permission() {
    struct TestCase {
        db_actions: Vec<String>,
        expected_db_actions: Vec<String>,
    }

    let cases = vec![
        TestCase {
            db_actions: vec!["write".to_owned(), "read".to_owned()],
            expected_db_actions: vec!["write".to_owned(), "read".to_owned(), "create".to_owned()],
        },
        TestCase {
            db_actions: vec!["write".to_owned()],
            expected_db_actions: vec!["write".to_owned(), "create".to_owned()],
        },
        TestCase {
            db_actions: vec!["read".to_owned()],
            expected_db_actions: vec!["read".to_owned()],
        },
    ];

    for case in cases {
        let mut v2_permissions = vec![PermissionDetails {
            resource_type: "db".to_owned(),
            resource_identifier: vec!["*".to_owned()],
            actions: case.db_actions,
        }];

        append_create_if_v2_write_action(&mut v2_permissions);

        let permission = v2_permissions.first().unwrap();
        assert_eq!(case.expected_db_actions, permission.actions);
    }
}

#[test]
fn test_append_token_with_db_foo_write_read_permissions() {
    let mut v2_permissions = vec![PermissionDetails {
        resource_type: "db".to_owned(),
        resource_identifier: vec!["foo".to_owned()],
        actions: vec!["write".to_owned(), "read".to_owned()],
    }];

    append_create_if_v2_write_action(&mut v2_permissions);

    let permission = v2_permissions.first().unwrap();
    assert_eq!(2, permission.actions.len());
    assert_eq!(
        vec!["write".to_owned(), "read".to_owned()],
        permission.actions
    );
}
