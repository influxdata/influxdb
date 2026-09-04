use super::*;
use crate::cas_test_stores::{LostResponseError, LostResponseStore};
use crate::test_object_store::{ErrorConfig, OperationKind, TestObjectStore};
use metric::Metric;
use object_store::UpdateVersion;
use object_store::memory::InMemory;

#[test]
fn metadata_key_is_portable_across_providers() {
    // Azure metadata names must be valid C# identifiers.
    assert!(!PUT_NONCE_METADATA_KEY.contains('-'));
    assert_eq!(
        PUT_NONCE_METADATA_KEY,
        PUT_NONCE_METADATA_KEY.to_lowercase()
    );
    assert!(PUT_NONCE_METADATA_KEY.starts_with(|c: char| c.is_ascii_alphabetic()));
    assert!(
        PUT_NONCE_METADATA_KEY
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
}

#[test]
fn generated_nonces_differ() {
    assert_ne!(PutNonce::generate(), PutNonce::generate());
}

#[test]
fn put_options_carry_the_marker_and_create_mode() {
    let nonce = PutNonce::generate();
    let opts = SelfVerifyingCreate::with_nonce(nonce.clone()).put_options();

    assert!(matches!(opts.mode, PutMode::Create));
    assert!(
        opts.attributes.is_empty(),
        "the layer adds attributes, not the caller"
    );

    let marker = opts
        .extensions
        .get::<SelfVerifyingCreate>()
        .expect("marker present");
    assert_eq!(marker.clone().into_nonce(), nonce);
}

#[test]
fn marker_without_a_nonce_mints_one() {
    let a = SelfVerifyingCreate::new().into_nonce();
    let b = SelfVerifyingCreate::new().into_nonce();
    assert_ne!(a, b);
}

#[test]
fn nonce_check_classifies_all_three_cases() {
    let ours = PutNonce::generate();
    let theirs = PutNonce::generate();

    let mut attrs = Attributes::new();
    assert_eq!(nonce_check(&attrs, &ours), NonceCheck::Absent);

    attrs.insert(
        Attribute::Metadata(PUT_NONCE_METADATA_KEY.into()),
        theirs.as_str().to_string().into(),
    );
    assert_eq!(nonce_check(&attrs, &ours), NonceCheck::Foreign);

    attrs.insert(
        Attribute::Metadata(PUT_NONCE_METADATA_KEY.into()),
        ours.as_str().to_string().into(),
    );
    assert_eq!(nonce_check(&attrs, &ours), NonceCheck::Ours);
}

fn store() -> (Arc<LostResponseStore>, SelfVerifyingCreateStore, Registry) {
    let inner = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let registry = Registry::new();
    let layer = SelfVerifyingCreateStore::new(Arc::clone(&inner) as _, &registry);
    (inner, layer, registry)
}

fn count(registry: &Registry, name: &'static str) -> u64 {
    registry
        .get_instrument::<Metric<U64Counter>>(name)
        .expect("counter registered")
        .get_observer(&metric::Attributes::from(&[]))
        .expect("series registered at construction")
        .fetch()
}

#[tokio::test]
async fn lost_response_on_a_marked_create_reports_success() {
    let (inner, layer, _registry) = store();
    let path = Path::from("wal/0001.wal");
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let result = layer
        .put_opts(
            &path,
            PutPayload::from_static(b"wal bytes"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(
        result.is_ok(),
        "own write reported as a conflict: {result:?}"
    );
}

/// S3 and GCS translate a conditional-create 412 into `AlreadyExists`;
/// Azure passes the status through, so the same collision arrives as
/// `Precondition`. Both must be verified, or the fix is S3/GCS-only.
#[tokio::test]
async fn precondition_on_a_marked_create_is_also_verified() {
    for shape in [
        LostResponseError::Precondition,
        LostResponseError::AlreadyExists,
    ] {
        let (inner, layer, _registry) = store();
        inner.lose_next_put_response_with(shape);

        let result = layer
            .put_opts(
                &Path::from("wal/0001.wal"),
                PutPayload::from_static(b"wal bytes"),
                SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
            )
            .await;

        assert!(
            result.is_ok(),
            "own write reported as a conflict for {shape:?}: {result:?}"
        );
    }
}

/// Pins the read-back shape: the nonce lives in the object's metadata, and
/// asking for a range of an object of unknown length can fail on its own.
#[tokio::test]
async fn read_back_is_metadata_only() {
    let (inner, layer, _registry) = store();
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    layer
        .put_opts(
            &Path::from("wal/0001.wal"),
            PutPayload::from_static(b"wal bytes"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap();

    let (range, head) = inner.last_get().expect("the layer read the object back");
    assert!(head, "read-back requested the object body");
    assert_eq!(range, None);
}

/// A ranged read-back is rejected against a zero-length object, so an empty
/// object at the path would leave every collision unverifiable.
#[tokio::test]
async fn a_zero_length_object_is_verified() {
    let (inner, layer, _registry) = store();
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let result = layer
        .put_opts(
            &Path::from("wal/0001.wal"),
            PutPayload::new(),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(
        result.is_ok(),
        "own zero-length write reported as a conflict: {result:?}"
    );
}

/// Azure surfaces an `If-None-Match: *` collision as `Precondition`, which
/// a caller keying its fail-stop on `AlreadyExists` would never see.
#[tokio::test]
async fn a_confirmed_conflict_is_reported_as_already_exists_on_every_backend() {
    for shape in [
        LostResponseError::Precondition,
        LostResponseError::AlreadyExists,
    ] {
        let (inner, layer, registry) = store();
        inner.report_collisions_as(shape);
        let path = Path::from("wal/0001.wal");

        // A different writer gets there first, tagging with its own nonce.
        layer
            .put_opts(
                &path,
                PutPayload::from_static(b"theirs"),
                SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
            )
            .await
            .unwrap();

        let result = layer
            .put_opts(
                &path,
                PutPayload::from_static(b"ours"),
                SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
            )
            .await;

        assert!(
            matches!(result, Err(object_store::Error::AlreadyExists { .. })),
            "collision shaped as {shape:?} was not normalised: {result:?}"
        );
        assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 1);
    }
}

#[tokio::test]
async fn counters_separate_self_writes_from_conflicts() {
    let (inner, layer, registry) = store();

    // Every series exists from construction, so a healthy node exports
    // zeroes rather than nothing at all.
    assert_eq!(count(&registry, SELF_WRITE_METRIC_NAME), 0);
    assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 0);
    assert_eq!(count(&registry, UNTAGGED_CONFLICT_METRIC_NAME), 0);
    assert_eq!(count(&registry, UNVERIFIED_CONFLICT_METRIC_NAME), 0);

    // Our own write, seen as a collision.
    let ours = Path::from("wal/0001.wal");
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);
    layer
        .put_opts(
            &ours,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap();

    assert_eq!(count(&registry, SELF_WRITE_METRIC_NAME), 1);
    assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 0);

    // Someone else's object at a second path.
    let theirs = Path::from("wal/0002.wal");
    layer
        .put_opts(
            &theirs,
            PutPayload::from_static(b"theirs"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap();
    layer
        .put_opts(
            &theirs,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap_err();

    assert_eq!(count(&registry, SELF_WRITE_METRIC_NAME), 1);
    assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 1);

    // An object nobody tagged, at a third path.
    let untagged = Path::from("wal/0003.wal");
    inner
        .put(&untagged, PutPayload::from_static(b"pre-upgrade"))
        .await
        .unwrap();
    layer
        .put_opts(
            &untagged,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap_err();

    assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 1);
    assert_eq!(count(&registry, UNTAGGED_CONFLICT_METRIC_NAME), 1);
    assert_eq!(count(&registry, UNVERIFIED_CONFLICT_METRIC_NAME), 0);
}

#[tokio::test]
async fn a_genuine_conflict_still_reports_already_exists() {
    let (_inner, layer, _registry) = store();
    let path = Path::from("wal/0001.wal");

    // A different writer gets there first, tagging with its own nonce.
    layer
        .put_opts(
            &path,
            PutPayload::from_static(b"theirs"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await
        .unwrap();

    let result = layer
        .put_opts(
            &path,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(matches!(
        result,
        Err(object_store::Error::AlreadyExists { .. })
    ));
}

#[tokio::test]
async fn an_untagged_object_reports_already_exists() {
    let (inner, layer, _registry) = store();
    let path = Path::from("wal/0001.wal");
    inner
        .put(&path, PutPayload::from_static(b"pre-upgrade"))
        .await
        .unwrap();

    let result = layer
        .put_opts(
            &path,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(matches!(
        result,
        Err(object_store::Error::AlreadyExists { .. })
    ));
}

/// A store built without the layer drops the marker: it writes no
/// attributes, so a `LocalFileSystem` would still accept the put, and a
/// collision reaches the caller as the backend shaped it.
#[tokio::test]
async fn an_unwrapped_store_ignores_the_marker() {
    let inner = LostResponseStore::new(Arc::new(InMemory::new()));
    let path = Path::from("wal/0001.wal");
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let result = inner
        .put_opts(
            &path,
            PutPayload::from_static(b"wal bytes"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(matches!(
        result,
        Err(object_store::Error::AlreadyExists { .. })
    ));

    let stored = inner.get(&path).await.unwrap();
    assert!(stored.attributes.is_empty());
}

#[tokio::test]
async fn an_unmarked_create_is_forwarded_untouched() {
    let (inner, layer, _registry) = store();
    let path = Path::from("wal/0001.wal");

    layer
        .put_opts(
            &path,
            PutPayload::from_static(b"plain"),
            PutOptions::from(PutMode::Create),
        )
        .await
        .unwrap();

    let stored = inner.get(&path).await.unwrap();
    assert!(stored.attributes.is_empty());
}

/// Read-backs the layer cannot perform must not soften a collision: a
/// caller that fail-stops on a duplicate writer would otherwise retry
/// forever while a second writer holds the path.
#[tokio::test]
async fn a_failed_read_back_reports_already_exists() {
    let lost = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let unreadable = Arc::new(
        TestObjectStore::new(Arc::clone(&lost) as _)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(|ctx| matches!(ctx.kind, OperationKind::GetOpts)),
    );
    let registry = Registry::new();
    let layer = SelfVerifyingCreateStore::new(unreadable as _, &registry);

    lost.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let result = layer
        .put_opts(
            &Path::from("wal/0001.wal"),
            PutPayload::from_static(b"wal bytes"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(
        matches!(result, Err(object_store::Error::AlreadyExists { .. })),
        "unverifiable conflict was softened: {result:?}"
    );
    assert_eq!(count(&registry, UNVERIFIED_CONFLICT_METRIC_NAME), 1);
    assert_eq!(count(&registry, CONFLICT_METRIC_NAME), 0);
}

#[tokio::test]
async fn a_read_back_that_fails_once_is_retried() {
    let lost = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let flaky = Arc::new(
        TestObjectStore::new(Arc::clone(&lost) as _)
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_failure_predicate(|ctx| matches!(ctx.kind, OperationKind::GetOpts)),
    );
    let registry = Registry::new();
    let layer = SelfVerifyingCreateStore::new(flaky as _, &registry);

    lost.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let result = layer
        .put_opts(
            &Path::from("wal/0001.wal"),
            PutPayload::from_static(b"wal bytes"),
            SelfVerifyingCreate::with_nonce(PutNonce::generate()).put_options(),
        )
        .await;

    assert!(
        result.is_ok(),
        "a transient read-back failure was not retried: {result:?}"
    );
    assert_eq!(count(&registry, SELF_WRITE_METRIC_NAME), 1);
    assert_eq!(count(&registry, UNVERIFIED_CONFLICT_METRIC_NAME), 0);
}

#[tokio::test]
async fn a_marked_overwrite_is_forwarded_untouched() {
    let (inner, layer, _registry) = store();
    let path = Path::from("wal/0001.wal");

    let mut opts = PutOptions::from(PutMode::Overwrite);
    opts.extensions
        .insert(SelfVerifyingCreate::with_nonce(PutNonce::generate()));
    layer
        .put_opts(&path, PutPayload::from_static(b"plain"), opts)
        .await
        .unwrap();

    let stored = inner.get(&path).await.unwrap();
    assert!(stored.attributes.is_empty());
}

/// A conditional update that loses its race is a lost race, even where the
/// object at the path carries this writer's nonce from an earlier create.
#[tokio::test]
async fn a_marked_update_that_loses_is_not_verified() {
    let (_inner, layer, registry) = store();
    let path = Path::from("wal/0001.wal");
    let nonce = PutNonce::generate();

    layer
        .put_opts(
            &path,
            PutPayload::from_static(b"ours"),
            SelfVerifyingCreate::with_nonce(nonce.clone()).put_options(),
        )
        .await
        .unwrap();

    let mut opts = PutOptions::from(PutMode::Update(UpdateVersion {
        e_tag: Some("stale".to_string()),
        version: None,
    }));
    opts.extensions
        .insert(SelfVerifyingCreate::with_nonce(nonce));
    let result = layer
        .put_opts(&path, PutPayload::from_static(b"newer"), opts)
        .await;

    assert!(
        matches!(result, Err(object_store::Error::Precondition { .. })),
        "a lost update was verified as this writer's own object: {result:?}"
    );
    assert_eq!(count(&registry, SELF_WRITE_METRIC_NAME), 0);
}
