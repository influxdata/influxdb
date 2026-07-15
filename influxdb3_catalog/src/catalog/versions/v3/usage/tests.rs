use super::*;

fn usage(dbs: u32, tables: u32, columns: u32) -> CurrentCatalogUsage {
    CurrentCatalogUsage::new(dbs as usize, tables as usize, columns as usize)
}

// ---------------------------------------------------------------------------
// CatalogLimits
// ---------------------------------------------------------------------------

#[test]
fn catalog_limits_none_is_effectively_unbounded() {
    let limits = CatalogLimits::none();
    let u = CurrentCatalogUsage::default();
    assert_eq!(limits.database_count_limit(&u), usize::MAX);
    assert_eq!(limits.table_count_limit(&u), usize::MAX);
    assert_eq!(limits.column_per_table_limit(&u), usize::MAX);
}

#[test]
fn catalog_limits_new_returns_stored_values_regardless_of_usage() {
    let limits = CatalogLimits::new(7, 11, 13);
    // Vary usage to confirm static limiter ignores it.
    for u in [usage(0, 0, 0), usage(50, 500, 5_000)] {
        assert_eq!(limits.database_count_limit(&u), 7);
        assert_eq!(limits.table_count_limit(&u), 11);
        assert_eq!(limits.column_per_table_limit(&u), 13);
        assert_eq!(limits.tag_column_per_table_limit(&u), NUM_TAG_COLUMNS_LIMIT);
    }
}

#[test]
fn catalog_limits_tag_column_override_returns_stored_value() {
    let limits = CatalogLimits::new(7, 11, 13).with_tag_columns_per_table_limit(5);
    for u in [usage(0, 0, 0), usage(50, 500, 5_000)] {
        assert_eq!(limits.tag_column_per_table_limit(&u), 5);
    }
}

// ---------------------------------------------------------------------------
// MaximumColumnCountLimiter
// ---------------------------------------------------------------------------

#[test]
fn maximum_column_count_default_uses_catalog_constant() {
    let limiter = MaximumColumnCountLimiter::default();
    assert_eq!(limiter.max_total_columns, Catalog::MAX_TOTAL_COLUMNS);
    let u = usage(0, 0, 0);
    // Empty catalog: full budget available as headroom for db/table/column.
    assert_eq!(
        limiter.database_count_limit(&u),
        Catalog::MAX_TOTAL_COLUMNS as usize,
    );
    assert_eq!(
        limiter.table_count_limit(&u),
        Catalog::MAX_TOTAL_COLUMNS as usize,
    );
    assert_eq!(
        limiter.column_per_table_limit(&u),
        Catalog::MAX_TOTAL_COLUMNS as usize,
    );
    assert_eq!(
        limiter.tag_column_per_table_limit(&u),
        NUM_TAG_COLUMNS_LIMIT
    );
}

#[test]
fn maximum_column_count_custom_creation() {
    let limiter = MaximumColumnCountLimiter::new(1000);
    let u = usage(10, 50, 500);
    assert_eq!(limiter.max_total_columns, 1000);
    assert_eq!(limiter.database_count_limit(&u), 510);
    assert_eq!(limiter.table_count_limit(&u), 550);
    assert_eq!(limiter.column_per_table_limit(&u), 500);
    assert_eq!(
        limiter.tag_column_per_table_limit(&u),
        NUM_TAG_COLUMNS_LIMIT
    );
}

#[test]
fn maximum_column_count_boundary_under_at_over() {
    let limiter = MaximumColumnCountLimiter::new(1000);

    // Under: returns current count + remaining budget.
    let under = usage(5, 20, 999);
    assert_eq!(limiter.database_count_limit(&under), 6);
    assert_eq!(limiter.table_count_limit(&under), 21);
    assert_eq!(limiter.column_per_table_limit(&under), 1);

    // At: returns current count; column_per_table is 0.
    let at = usage(5, 20, 1000);
    assert_eq!(limiter.database_count_limit(&at), 5);
    assert_eq!(limiter.table_count_limit(&at), 20);
    assert_eq!(limiter.column_per_table_limit(&at), 0);

    // Over: same as at — current count; column_per_table is 0.
    let over = usage(5, 20, 1001);
    assert_eq!(limiter.database_count_limit(&over), 5);
    assert_eq!(limiter.table_count_limit(&over), 20);
    assert_eq!(limiter.column_per_table_limit(&over), 0);
}

#[test]
fn maximum_column_count_zero_and_one_limits() {
    // Zero limit: nothing fits. With zero usage, "at" the limit (not under).
    let limiter_zero = MaximumColumnCountLimiter::new(0);
    let zero_usage = usage(0, 0, 0);
    assert_eq!(limiter_zero.database_count_limit(&zero_usage), 0);
    assert_eq!(limiter_zero.table_count_limit(&zero_usage), 0);
    assert_eq!(limiter_zero.column_per_table_limit(&zero_usage), 0);
    // Non-zero usage with zero limit: over limit.
    let nonzero_usage = usage(1, 1, 1);
    assert_eq!(limiter_zero.database_count_limit(&nonzero_usage), 1);
    assert_eq!(limiter_zero.table_count_limit(&nonzero_usage), 1);
    assert_eq!(limiter_zero.column_per_table_limit(&nonzero_usage), 0);

    // Limit of 1.
    let limiter_one = MaximumColumnCountLimiter::new(1);
    // Empty: under limit, headroom = 1.
    assert_eq!(limiter_one.database_count_limit(&zero_usage), 1);
    assert_eq!(limiter_one.table_count_limit(&zero_usage), 1);
    assert_eq!(limiter_one.column_per_table_limit(&zero_usage), 1);
    // One column: at limit.
    let one_usage = usage(1, 1, 1);
    assert_eq!(limiter_one.database_count_limit(&one_usage), 1);
    assert_eq!(limiter_one.table_count_limit(&one_usage), 1);
    assert_eq!(limiter_one.column_per_table_limit(&one_usage), 0);
    // Two columns: over limit.
    let two_usage = usage(2, 2, 2);
    assert_eq!(limiter_one.database_count_limit(&two_usage), 2);
    assert_eq!(limiter_one.table_count_limit(&two_usage), 2);
    assert_eq!(limiter_one.column_per_table_limit(&two_usage), 0);
}

#[test]
fn maximum_column_count_large_values() {
    // u32::MAX / 2 is the practical cap: every table needs at least one
    // column, so the table count is bounded by the column budget.
    let limiter = MaximumColumnCountLimiter::new(u32::MAX / 2);

    let under = usage(100, 1000, u32::MAX / 2 - 2);
    let at = usage(100, 1000, u32::MAX / 2);
    let over = usage(100, 1000, u32::MAX / 2 + 1);

    // Under: remaining headroom is 2.
    assert_eq!(limiter.database_count_limit(&under), 100 + 2);
    assert_eq!(limiter.table_count_limit(&under), 1000 + 2);
    assert_eq!(limiter.column_per_table_limit(&under), 2);

    // At: returns current.
    assert_eq!(limiter.database_count_limit(&at), 100);
    assert_eq!(limiter.table_count_limit(&at), 1000);
    assert_eq!(limiter.column_per_table_limit(&at), 0);

    // Over: returns current.
    assert_eq!(limiter.database_count_limit(&over), 100);
    assert_eq!(limiter.table_count_limit(&over), 1000);
    assert_eq!(limiter.column_per_table_limit(&over), 0);

    // Pathological: tables already maxed at the column budget. We still
    // return current_table_count without overflow.
    let absurd = usage(100, u32::MAX / 2, u32::MAX / 2);
    assert_eq!(limiter.table_count_limit(&absurd), (u32::MAX / 2) as usize);
}
