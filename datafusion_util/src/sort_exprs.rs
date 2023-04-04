use datafusion::{
    arrow::compute::SortOptions,
    physical_expr::{PhysicalSortExpr, PhysicalSortRequirement},
};

/// Structure to build [`PhysicalSortRequirement`]s for ExecutionPlans.
///
/// Replace with `PhysicalSortExpr::from_sort_exprs` when
/// <https://github.com/apache/arrow-datafusion/pull/5863> is merged
/// upstream.
pub fn requirements_from_sort_exprs<'a>(
    exprs: impl IntoIterator<Item = &'a PhysicalSortExpr>,
) -> Vec<PhysicalSortRequirement> {
    exprs
        .into_iter()
        .cloned()
        .map(PhysicalSortRequirement::from)
        .collect()
}

/// Converts the `PhysicalSortRequirement` to `PhysicalSortExpr`.
/// If required ordering is `None` for an entry, the default
/// ordering `ASC, NULLS LAST` is used.
///
/// The default is picked to be consistent with
/// PostgreSQL: <https://www.postgresql.org/docs/current/queries-order.html>
///
/// Replace with `PhysicalSortExpr::from` when
/// <https://github.com/apache/arrow-datafusion/pull/5863> is merged
/// upstream.
pub fn into_sort_expr(requirement: PhysicalSortRequirement) -> PhysicalSortExpr {
    let PhysicalSortRequirement { expr, options } = requirement;

    let options = options.unwrap_or(SortOptions {
        descending: false,
        nulls_first: false,
    });
    PhysicalSortExpr { expr, options }
}

/// This function converts `PhysicalSortRequirement` to `PhysicalSortExpr`
/// for each entry in the input. If required ordering is None for an entry
/// default ordering `ASC, NULLS LAST` if given.
///
/// replace with PhysicalSortExpr::to_sort_exprs when
/// <https://github.com/apache/arrow-datafusion/pull/5863> is merged
/// upstream.
pub fn requirements_to_sort_exprs(
    required: impl IntoIterator<Item = PhysicalSortRequirement>,
) -> Vec<PhysicalSortExpr> {
    required.into_iter().map(into_sort_expr).collect()
}
