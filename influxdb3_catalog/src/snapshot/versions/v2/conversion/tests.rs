use influxdb3_authz::DatabaseActions;

use crate::snapshot::versions::{v2, v3};

impl v3::ActionsSnapshot {
    fn get_db_value(&self) -> DatabaseActions {
        match self {
            v3::ActionsSnapshot::Database(database_actions_snapshot) => {
                DatabaseActions::from(database_actions_snapshot.0)
            }
            _ => panic!("unexpected variant, only unwrap db variant"),
        }
    }
}

#[test]
fn test_conversion_of_db_tokens_from_v2_to_v3() {
    struct TestCase {
        v2_db_actions: Vec<String>,
        v3_db_actions: Vec<String>,
    }

    let cases = vec![
        TestCase {
            v2_db_actions: vec!["write".to_owned(), "read".to_owned()],
            v3_db_actions: vec!["write".to_owned(), "read".to_owned(), "create".to_owned()],
        },
        TestCase {
            v2_db_actions: vec!["read".to_owned()],
            v3_db_actions: vec!["read".to_owned()],
        },
        TestCase {
            v2_db_actions: vec!["write".to_owned()],
            v3_db_actions: vec!["write".to_owned(), "create".to_owned()],
        },
    ];

    for case in cases {
        let database_actions =
            DatabaseActions::new(&case.v2_db_actions).expect("actions to be parsed");
        let v2_permission = v2::PermissionSnapshot {
            resource_type: v2::ResourceTypeSnapshot::Database,
            resource_identifier: v2::ResourceIdentifierSnapshot::Wildcard,
            actions: v2::ActionsSnapshot::Database(v2::DatabaseActionsSnapshot(
                database_actions.as_u16(),
            )),
        };

        let expected_db_actions_with_create =
            DatabaseActions::new(&case.v3_db_actions).expect("expected actions to be parsed");
        let v3_permission: v3::PermissionSnapshot = v2_permission.into();
        assert!(matches!(
            v3_permission.resource_type,
            v3::ResourceTypeSnapshot::Database
        ));
        assert!(matches!(
            v3_permission.resource_identifier,
            v3::ResourceIdentifierSnapshot::Wildcard
        ));
        assert_eq!(
            v3_permission.actions.get_db_value().as_u16(),
            expected_db_actions_with_create.as_u16()
        );
    }
}
