use super::*;
use influxdb3_id::DbId;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestResource {
    id: DbId,
    name: Arc<str>,
}

impl CatalogResource for TestResource {
    type Identifier = DbId;
    const CATEGORY: &'static str = "test";
    fn id(&self) -> Self::Identifier {
        self.id
    }
    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

fn repo_with_one() -> (Repository<DbId, TestResource>, DbId) {
    let mut repo = Repository::<DbId, TestResource>::new();
    let id = DbId::new(0);
    repo.insert(
        id,
        TestResource {
            id,
            name: "original".into(),
        },
    )
    .unwrap();
    (repo, id)
}

#[test]
fn modify_by_id_writes_back_renamed_resource_and_keeps_map_consistent() {
    let (mut repo, id) = repo_with_one();
    repo.modify_by_id::<(), RepositoryError<DbId>>(&id, |r| {
        r.name = "renamed".into();
        Ok(())
    })
    .unwrap();

    assert_eq!(repo.get_by_id(&id).unwrap().name().as_ref(), "renamed");
    assert_eq!(repo.name_to_id("renamed"), Some(id));
    assert_eq!(repo.name_to_id("original"), None);
    assert_eq!(repo.id_to_name(&id).as_deref(), Some("renamed"));
}

#[test]
fn modify_by_id_returns_not_found_for_missing_id() {
    let mut repo = Repository::<DbId, TestResource>::new();
    let err = repo
        .modify_by_id::<(), RepositoryError<DbId>>(&DbId::new(42), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { .. }));
}

#[test]
fn modify_by_id_rolls_back_when_closure_renames_then_fails() {
    let (mut repo, id) = repo_with_one();
    let err = repo
        .modify_by_id::<(), RepositoryError<DbId>>(&id, |r| {
            // Rename, then fail: the rename must not leak into the repo or
            // the id↔name map.
            r.name = "renamed".into();
            Err(RepositoryError::NotFound {
                resource: "test",
                id: DbId::new(0),
            })
        })
        .unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { .. }));
    assert_eq!(repo.get_by_id(&id).unwrap().name().as_ref(), "original");
    assert_eq!(repo.name_to_id("original"), Some(id));
    assert_eq!(repo.name_to_id("renamed"), None);
}

#[test]
fn modify_by_id_rejects_rename_onto_existing_name() {
    let mut repo = Repository::<DbId, TestResource>::new();
    let a = DbId::new(0);
    let b = DbId::new(1);
    repo.insert(
        a,
        TestResource {
            id: a,
            name: "a".into(),
        },
    )
    .unwrap();
    repo.insert(
        b,
        TestResource {
            id: b,
            name: "b".into(),
        },
    )
    .unwrap();

    // Renaming `a` onto `b`'s name must be rejected, not silently desync the
    // id↔name map (which would later panic in `id_exists`).
    let err = repo
        .modify_by_id::<(), RepositoryError<DbId>>(&a, |r| {
            r.name = "b".into();
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, RepositoryError::AlreadyExistsByName { .. }));

    // Both resources are untouched and the map is still consistent.
    assert_eq!(repo.get_by_id(&a).unwrap().name().as_ref(), "a");
    assert_eq!(repo.get_by_id(&b).unwrap().name().as_ref(), "b");
    assert_eq!(repo.name_to_id("a"), Some(a));
    assert_eq!(repo.name_to_id("b"), Some(b));
}

#[test]
fn modify_by_id_returns_closure_value() {
    let (mut repo, id) = repo_with_one();
    let got = repo
        .modify_by_id::<u32, RepositoryError<DbId>>(&id, |_| Ok(7))
        .unwrap();
    assert_eq!(got, 7);
}

#[test]
fn modify_by_id_in_place_mutates_without_reallocating() {
    let (mut repo, id) = repo_with_one();
    let before = Arc::as_ptr(&repo.get_by_id(&id).unwrap());

    repo.modify_by_id_in_place::<(), RepositoryError<DbId>>(&id, |r| {
        r.name = "renamed".into();
        Ok(())
    })
    .unwrap();

    // Nothing else held the resource, so `Arc::make_mut` wrote through the
    // existing allocation instead of deep-copying it — this is the whole point
    // of the method, so assert on the allocation rather than just the value.
    assert_eq!(Arc::as_ptr(&repo.get_by_id(&id).unwrap()), before);
    assert_eq!(repo.get_by_id(&id).unwrap().name().as_ref(), "renamed");
    assert_eq!(repo.name_to_id("renamed"), Some(id));
    assert_eq!(repo.name_to_id("original"), None);
    assert_eq!(repo.id_to_name(&id).as_deref(), Some("renamed"));
}

#[test]
fn modify_by_id_in_place_leaves_reader_snapshots_frozen() {
    let (mut repo, id) = repo_with_one();
    // A reader holding an `Arc` is exactly what the catalog hands out from
    // `db_schema()`; it must keep observing the state it was given.
    let snapshot = repo.get_by_id(&id).unwrap();

    repo.modify_by_id_in_place::<(), RepositoryError<DbId>>(&id, |r| {
        r.name = "renamed".into();
        Ok(())
    })
    .unwrap();

    assert_eq!(snapshot.name().as_ref(), "original");
    assert_eq!(repo.get_by_id(&id).unwrap().name().as_ref(), "renamed");
    assert_ne!(
        Arc::as_ptr(&repo.get_by_id(&id).unwrap()),
        Arc::as_ptr(&snapshot),
        "the repository should have copied on write, not mutated the snapshot"
    );
}

#[test]
fn modify_by_id_in_place_returns_not_found_for_missing_id() {
    let mut repo = Repository::<DbId, TestResource>::new();
    let err = repo
        .modify_by_id_in_place::<(), RepositoryError<DbId>>(&DbId::new(42), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { .. }));
}

#[test]
fn modify_by_id_in_place_rejects_rename_onto_existing_name() {
    let (mut repo, a) = repo_with_one();
    let b = DbId::new(1);
    repo.insert(
        b,
        TestResource {
            id: b,
            name: "taken".into(),
        },
    )
    .unwrap();

    let err = repo
        .modify_by_id_in_place::<(), RepositoryError<DbId>>(&a, |r| {
            r.name = "taken".into();
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, RepositoryError::AlreadyExistsByName { .. }));
    // The rename cannot be undone once applied in place, but the bijection must
    // survive intact: `b` keeps its name, and `a` is still indexed (under the
    // name it had going in) rather than being dropped from the map entirely.
    assert_eq!(repo.name_to_id("taken"), Some(b));
    assert_eq!(repo.id_to_name(&a).as_deref(), Some("original"));
    assert!(repo.id_exists(&a));
}

#[test]
fn modify_by_id_in_place_returns_closure_value() {
    let (mut repo, id) = repo_with_one();
    let out = repo
        .modify_by_id_in_place::<u32, RepositoryError<DbId>>(&id, |_| Ok(7))
        .unwrap();
    assert_eq!(out, 7);
}

#[test]
fn insert_with_already_used_name_fails() {
    let (mut repo, id) = repo_with_one();
    let name = repo.get_by_id(&id).unwrap().name();

    repo.insert(
        DbId::new(20),
        TestResource {
            id: DbId::new(20),
            name,
        },
    )
    .unwrap_err();

    repo.insert(
        id,
        TestResource {
            id,
            name: Arc::from("other_name"),
        },
    )
    .unwrap_err();
}

// This is an important property, even though it seems kinda weird an arbitrary.
//
// For a while, there was a bug in the `Repository` implementation that allowed duplicate names (see
// https://github.com/influxdata/influxdb_pro/pull/5750), and that caused the creation of a lot of
// catalogs with seemingly-inconsistent repository states (see
// https://github.com/influxdata/influxdb_pro/issues/6281). So now we have to allow this specific
// case of duplication to ensure that these older catalogs keep loading while also preventing the
// least amount of future duplication.
#[test]
fn insert_allowing_duplicate_name_keeps_first_holder_of_the_name() {
    let (mut repo, first) = repo_with_one();
    let name = repo.get_by_id(&first).unwrap().name();
    let second = DbId::new(20);

    let name_taken = repo
        .insert_allowing_duplicate_name(
            second,
            TestResource {
                id: second,
                name: Arc::clone(&name),
            },
        )
        .unwrap();
    assert!(name_taken);

    assert_eq!(repo.len(), 2);
    assert_eq!(repo.name_to_id(&name), Some(first));
    assert_eq!(repo.id_to_name(&second), None);
    assert!(repo.contains_id(&second));
    assert_eq!(repo.get_by_id(&second).unwrap().id, second);
    assert_eq!(repo.next_id(), DbId::new(21));

    // A fresh name goes in like a regular insert.
    let third = DbId::new(21);
    let name_taken = repo
        .insert_allowing_duplicate_name(
            third,
            TestResource {
                id: third,
                name: Arc::from("other"),
            },
        )
        .unwrap();
    assert!(!name_taken);
    assert_eq!(repo.name_to_id("other"), Some(third));

    // Ids are still unique, including the unnamed one.
    repo.insert_allowing_duplicate_name(
        second,
        TestResource {
            id: second,
            name: Arc::from("another"),
        },
    )
    .unwrap_err();
    repo.insert(
        second,
        TestResource {
            id: second,
            name: Arc::from("another"),
        },
    )
    .unwrap_err();
}

#[test]
fn unnamed_resource_takes_the_name_once_it_is_free() {
    let (mut repo, first) = repo_with_one();
    let name = repo.get_by_id(&first).unwrap().name();
    let second = DbId::new(20);
    repo.insert_allowing_duplicate_name(
        second,
        TestResource {
            id: second,
            name: Arc::clone(&name),
        },
    )
    .unwrap();

    // Updating the unnamed resource without a rename leaves it unnamed.
    repo.modify_by_id::<_, RepositoryError<DbId>>(&second, |r| {
        r.id = second;
        Ok(())
    })
    .unwrap();
    assert_eq!(repo.name_to_id(&name), Some(first));
    assert_eq!(repo.id_to_name(&second), None);
    repo.modify_by_id_in_place::<_, RepositoryError<DbId>>(&second, |_| Ok(()))
        .unwrap();
    assert_eq!(repo.id_to_name(&second), None);

    // Renaming it onto a name another id holds is still refused.
    let third = DbId::new(30);
    repo.insert(
        third,
        TestResource {
            id: third,
            name: Arc::from("third"),
        },
    )
    .unwrap();
    let err = repo
        .update(
            second,
            TestResource {
                id: second,
                name: Arc::from("third"),
            },
        )
        .unwrap_err();
    assert!(matches!(err, RepositoryError::AlreadyExistsByName { .. }));
    assert_eq!(repo.id_to_name(&second), None);
    assert_eq!(repo.name_to_id("third"), Some(third));

    // Once the holder is gone the unnamed resource can claim the name.
    repo.remove(&first);
    assert_eq!(repo.name_to_id(&name), None);
    repo.update(
        second,
        TestResource {
            id: second,
            name: Arc::clone(&name),
        },
    )
    .unwrap();
    assert_eq!(repo.name_to_id(&name), Some(second));
    assert!(repo.id_exists(&second));
}

#[test]
#[should_panic(expected = "id map and repository are in an inconsistent state")]
fn id_exists_still_rejects_an_unrecorded_unnamed_id() {
    let (mut repo, id) = repo_with_one();
    // A resource left without a name-map entry by anything else is still corruption.
    repo.id_name_map.remove_by_left(&id);
    repo.id_exists(&id);
}
