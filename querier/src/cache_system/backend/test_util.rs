use super::CacheBackend;

/// Generic test set for [`Backend`].
///
/// The backend must NOT perform any pruning/deletions during the tests (even though backends are allowed to do that in
/// general).
pub fn test_generic<B, F>(constructor: F)
where
    B: CacheBackend<K = u8, V = String>,
    F: Fn() -> B,
{
    test_get_empty(constructor());
    test_get_set(constructor());
    test_get_twice(constructor());
    test_override(constructor());
    test_set_remove_get(constructor());
    test_remove_empty(constructor());
    test_readd(constructor());
    test_is_empty(constructor());
}

/// Test GET on empty backend.
fn test_get_empty<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    assert_eq!(backend.get(&1), None);
}

/// Test GET and SET without any overrides.
fn test_get_set<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.set(1, String::from("a"));
    backend.set(2, String::from("b"));

    assert_eq!(backend.get(&1), Some(String::from("a")));
    assert_eq!(backend.get(&2), Some(String::from("b")));
    assert_eq!(backend.get(&3), None);
}

/// Test that a value can be retrieved multiple times.
fn test_get_twice<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.set(1, String::from("a"));

    assert_eq!(backend.get(&1), Some(String::from("a")));
    assert_eq!(backend.get(&1), Some(String::from("a")));
}

/// Test that setting a value twice w/o deletion overrides the existing value.
fn test_override<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.set(1, String::from("a"));
    backend.set(1, String::from("b"));

    assert_eq!(backend.get(&1), Some(String::from("b")));
}

/// Test removal of on empty backend.
fn test_remove_empty<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.remove(&1);
}

/// Test removal of existing values.
fn test_set_remove_get<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.set(1, String::from("a"));
    backend.remove(&1);

    assert_eq!(backend.get(&1), None);
}

/// Test setting a new value after removing it.
fn test_readd<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    backend.set(1, String::from("a"));
    backend.remove(&1);
    backend.set(1, String::from("b"));

    assert_eq!(backend.get(&1), Some(String::from("b")));
}

/// Test `is_empty` check.
fn test_is_empty<B>(mut backend: B)
where
    B: CacheBackend<K = u8, V = String>,
{
    assert!(backend.is_empty());

    backend.set(1, String::from("a"));
    backend.set(2, String::from("b"));
    assert!(!backend.is_empty());

    backend.remove(&1);
    assert!(!backend.is_empty());

    backend.remove(&2);
    assert!(backend.is_empty());
}
