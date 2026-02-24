use super::*;

#[test]
fn test_debug() {
    assert_eq!(
        format!("{:?}", MockStore::new()),
        "MockStore { state: Mutex { data: MockStoreState { calls: [], index_counter: 0 }, poisoned: false, .. } }",
    );
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", MockStore::new()), "mock",);
}

#[tokio::test]
async fn test_result_mocking() {
    let store = MockStore::new()
        .mock_next(MockCall::Copy {
            params: (path(), path()),
            barriers: vec![],
            res: Ok(()),
        })
        .mock_next(MockCall::Copy {
            params: (path(), path()),
            barriers: vec![],
            res: Err(err()),
        });

    store.copy(&path(), &path()).await.unwrap();
    store.copy(&path(), &path()).await.unwrap_err();
}

#[tokio::test]
#[should_panic(expected = "no mocked call left but store is called with Copy")]
async fn test_no_calls_left() {
    let store = MockStore::new().mock_next(MockCall::Copy {
        params: (path(), path()),
        barriers: vec![],
        res: Ok(()),
    });

    store.copy(&path(), &path()).await.unwrap();
    store.copy(&path(), &path()).await.ok();
}

#[tokio::test]
#[should_panic(expected = "next response is not a Rename but a Copy at 0-based-index 0")]
async fn test_wrong_calls_left() {
    let store = MockStore::new().mock_next(MockCall::Copy {
        params: (path(), path()),
        // Barrier is NOT checked.
        barriers: vec![Arc::new(Barrier::new(2))],
        res: Ok(()),
    });

    store.rename(&path(), &path()).await.ok();
}

#[tokio::test]
#[should_panic(
    expected = "mocked parameters are different from the actual ones for Copy at 0-based-index 0:"
)]
async fn test_params_checked() {
    let path2 = Path::parse("other").unwrap();
    let store = MockStore::new().mock_next(MockCall::Copy {
        params: (path(), path2),
        // Barrier is used AFTER param checking.
        barriers: vec![Arc::new(Barrier::new(2))],
        res: Ok(()),
    });

    store.copy(&path(), &path()).await.ok();
}

#[test]
#[should_panic(expected = "mocked calls left on drop")]
fn test_calls_left_drop() {
    MockStore::new().mock_next(MockCall::Copy {
        params: (path(), path()),
        barriers: vec![],
        res: Ok(()),
    });
}

/// Do NOT double-panic due to the "mock calls left on drop" test because this would abort the process and
/// potentially also hide the actual test failure -- at least the DX would be not great.
#[test]
#[should_panic(expected = "foo")]
fn test_calls_left_no_double_panic() {
    let _store = MockStore::new().mock_next(MockCall::Copy {
        params: (path(), path()),
        barriers: vec![],
        res: Ok(()),
    });
    panic!("foo")
}

#[test]
fn test_paths_different() {
    assert_ne!(path(), path2());
}
