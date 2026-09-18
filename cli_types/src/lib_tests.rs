use super::*;

#[test]
fn default_max_concurrent_queries_floors_small_nodes() {
    // Up to 12 units of parallelism the floor of 50 wins; above it the
    // per-core scale takes over.
    for parallelism in [1, 2, 8, 12] {
        assert_eq!(default_max_concurrent_queries(parallelism), 50);
    }
    assert_eq!(default_max_concurrent_queries(13), 52);
    assert_eq!(default_max_concurrent_queries(16), 64);
    assert_eq!(default_max_concurrent_queries(64), 256);
}

#[test]
fn resolve_max_concurrent_queries_prefers_configured_value() {
    assert_eq!(
        resolve_max_concurrent_queries(Some(MaxConcurrentQueries(7)), Some(16), 16),
        7
    );
}

#[test]
fn resolve_max_concurrent_queries_uses_effective_parallelism() {
    // Threads default to the core count.
    assert_eq!(resolve_max_concurrent_queries(None, None, 16), 64);
    // Threads capped below the cores lower the effective parallelism.
    assert_eq!(resolve_max_concurrent_queries(None, Some(4), 64), 50);
    // Threads above the cores add no parallelism.
    assert_eq!(resolve_max_concurrent_queries(None, Some(128), 32), 128);
}
