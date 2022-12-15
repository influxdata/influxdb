use iox_query::pruning::NotPrunedReason;
use metric::{Attributes, U64Counter};

#[derive(Debug)]
pub struct PruneMetricsGroup {
    /// number of chunks
    chunks: U64Counter,

    /// number of rows
    rows: U64Counter,

    /// estimated size in bytes
    bytes: U64Counter,
}

impl PruneMetricsGroup {
    fn new(metric_registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let attributes: Attributes = attributes.into();

        let chunks = metric_registry
            .register_metric::<U64Counter>(
                "query_pruner_chunks",
                "Number of chunks seen by the statistics-based chunk pruner",
            )
            .recorder(attributes.clone());

        let rows = metric_registry
            .register_metric::<U64Counter>(
                "query_pruner_rows",
                "Number of rows seen by the statistics-based chunk pruner",
            )
            .recorder(attributes.clone());

        let bytes = metric_registry
            .register_metric::<U64Counter>(
                "query_pruner_bytes",
                "Size (estimated bytes) of chunks seen by the statistics-based chunk pruner",
            )
            .recorder(attributes);

        Self {
            chunks,
            rows,
            bytes,
        }
    }

    pub fn inc(&self, chunks: u64, rows: u64, bytes: u64) {
        self.chunks.inc(chunks);
        self.rows.inc(rows);
        self.bytes.inc(bytes);
    }
}

#[derive(Debug)]
pub struct PruneMetrics {
    /// Chunks that have been pruned based on cheaply-available metadata.
    ///
    /// This was done before the actual [`QueryChunk`](iox_query::QueryChunk) was created because the latter needs some
    /// slightly more expensive data like the partition sort key.
    ///
    /// At the moment we can prune chunks early only based on "time".
    pub pruned_early: PruneMetricsGroup,

    /// Chunks that have been pruned after they have been created. At this stage we likely had better/more statistics available.
    pub pruned_late: PruneMetricsGroup,

    /// The pruning process worked but the chunk was not pruned and needs to be scanned.
    pub not_pruned: PruneMetricsGroup,

    /// We could not prune these chunks because there was no filter expression available.
    ///
    /// This may happen for "scan all" type of queries.
    pub could_not_prune_no_expression: PruneMetricsGroup,

    /// We could not prune these chunks because we were unable to create the DataFusion pruning predicate. This is most
    /// likely a missing feature in DataFusion.
    pub could_not_prune_cannot_create_predicate: PruneMetricsGroup,

    /// We could not prune these chunks because DataFusion failed to apply the pruning predicate to the chunks. This is
    /// most likely a missing feature in DataFusion.
    pub could_not_prune_df: PruneMetricsGroup,
}

impl PruneMetrics {
    pub fn new(metric_registry: &metric::Registry) -> Self {
        let pruned_early = PruneMetricsGroup::new(metric_registry, &[("result", "pruned_early")]);
        let pruned_late = PruneMetricsGroup::new(metric_registry, &[("result", "pruned_late")]);
        let not_pruned = PruneMetricsGroup::new(metric_registry, &[("result", "not_pruned")]);
        let could_not_prune_no_expression = PruneMetricsGroup::new(
            metric_registry,
            &[
                ("result", "could_not_prune"),
                ("reason", NotPrunedReason::NoExpressionOnPredicate.name()),
            ],
        );
        let could_not_prune_cannot_create_predicate = PruneMetricsGroup::new(
            metric_registry,
            &[
                ("result", "could_not_prune"),
                (
                    "reason",
                    NotPrunedReason::CanNotCreatePruningPredicate.name(),
                ),
            ],
        );
        let could_not_prune_df = PruneMetricsGroup::new(
            metric_registry,
            &[
                ("result", "could_not_prune"),
                (
                    "reason",
                    NotPrunedReason::CanNotCreatePruningPredicate.name(),
                ),
            ],
        );

        Self {
            pruned_early,
            pruned_late,
            not_pruned,
            could_not_prune_no_expression,
            could_not_prune_cannot_create_predicate,
            could_not_prune_df,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_unregistered() -> Self {
        Self::new(&metric::Registry::new())
    }
}
