use async_trait::async_trait;
use data_types::{
    partition_template::TablePartitionTemplateOverride, NamespaceName, NamespaceSchema,
    PartitionKey, TableId,
};
use hashbrown::HashMap;
use mutable_batch::{MutableBatch, PartitionKeyError, PartitionWrite, WritePayload};
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::DmlHandler;

/// An error raised by the [`Partitioner`] handler.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Failed to write to the partitioned table batch.
    #[error("error batching into partitioned write: {0}")]
    BatchWrite(#[from] mutable_batch::Error),

    /// An error deriving the partition key from the partition key template.
    #[error("error generating partition key: {0}")]
    Partitioner(#[from] PartitionKeyError),
}

/// A decorator of `T`, tagging it with the partition key derived from it.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Partitioned<T> {
    key: PartitionKey,
    payload: T,
}

impl<T> Partitioned<T> {
    /// Wrap `payload` with a partition `key`.
    pub fn new(key: PartitionKey, payload: T) -> Self {
        Self { key, payload }
    }

    /// Get a reference to the partition payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Unwrap `Self` returning the inner payload `T` and the partition key.
    pub fn into_parts(self) -> (PartitionKey, T) {
        (self.key, self.payload)
    }
}

/// A [`DmlHandler`] implementation that splits per-table [`MutableBatch`] into
/// partitioned per-table [`MutableBatch`] instances according to the tables' partition templates.
/// Deletes pass through unmodified.
///
/// A vector of partitions are returned to the caller, or the first error that
/// occurs during partitioning.
#[derive(Debug, Default)]
pub struct Partitioner {}

#[async_trait]
impl DmlHandler for Partitioner {
    type WriteError = PartitionError;

    type WriteInput = HashMap<TableId, (String, TablePartitionTemplateOverride, MutableBatch)>;
    type WriteOutput = Vec<Partitioned<HashMap<TableId, (String, MutableBatch)>>>;

    /// Partition the per-table [`MutableBatch`].
    async fn write(
        &self,
        _namespace: &NamespaceName<'static>,
        _namespace_schema: Arc<NamespaceSchema>,
        batch: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        // A collection of partition-keyed, per-table MutableBatch instances.
        let mut partitions: HashMap<PartitionKey, HashMap<_, (String, MutableBatch)>> =
            HashMap::default();

        for (table_id, (table_name, table_partition_template, batch)) in batch {
            // Partition the table batch according to the configured partition
            // template and write it into the partition-keyed map.
            for (partition_key, partition_payload) in
                PartitionWrite::partition(&batch, &table_partition_template)?
            {
                let partition = partitions.entry(partition_key).or_default();
                let table_batch = partition
                    .raw_entry_mut()
                    .from_key(&table_id)
                    .or_insert_with(|| {
                        (table_id, (table_name.to_owned(), MutableBatch::default()))
                    });

                partition_payload.write_to_batch(&mut table_batch.1 .1)?;
            }
        }

        Ok(partitions
            .into_iter()
            .map(|(key, batch)| Partitioned::new(key, batch))
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::{
        partition_template::{test_table_partition_override, TemplatePart},
        NamespaceId,
    };

    use super::*;

    // Parse `lp` into a table-keyed MutableBatch map.
    pub(crate) fn lp_to_writes(
        lp: &str,
    ) -> HashMap<TableId, (String, TablePartitionTemplateOverride, MutableBatch)> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");

        writes
            .into_iter()
            .enumerate()
            .map(|(i, (name, data))| (TableId::new(i as _), (name, Default::default(), data)))
            .collect()
    }

    // Start a new `NamespaceSchema` with only the given ID; the rest of the fields are arbitrary.
    fn namespace_schema(id: i64) -> Arc<NamespaceSchema> {
        Arc::new(NamespaceSchema {
            id: NamespaceId::new(id),
            tables: Default::default(),
            max_columns_per_table: 500,
            max_tables: 200,
            retention_period_ns: None,
            partition_template: Default::default(),
        })
    }

    // Generate a test case that partitions "lp".
    //
    // Assert the partition-to-table mapping in "want_writes" and assert the
    // handler write() return value in "want_handler_ret".
    macro_rules! test_write {
        (
            $name:ident,
            lp = $lp:expr,
            want_writes = [$($want_writes:tt)*], // "partition key" => ["mapped", "tables"] or [unchecked] to skip assert
            want_handler_ret = $($want_handler_ret:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_write_ $name>]() {
                    let partitioner = Partitioner::default();
                    let ns = NamespaceName::new("bananas").expect("valid db name");

                    let writes = lp_to_writes($lp);

                    let handler_ret = partitioner.write(
                        &ns,
                        namespace_schema(42),
                        writes,
                        None
                    ).await;
                    assert_matches!(handler_ret, $($want_handler_ret)+);

                    // Check the partition -> table mapping.
                    let got = handler_ret.unwrap_or_default()
                        .into_iter()
                        .map(|partition| {
                            // Extract the table names in this partition
                            let mut tables = partition
                                .payload
                                .values().map(|v| v.0.clone())
                                .collect::<Vec<String>>();

                            tables.sort();

                            (partition.key.clone(), tables)
                        })
                        .collect::<HashMap<_, _>>();

                    test_write!(@assert_writes, got, $($want_writes)*);
                }
            }
        };

        // Generate a NOP that doesn't assert the writes if "unchecked" is
        // specified.
        //
        // This is useful for tests that cause non-deterministic partial writes.
        (@assert_writes, $got:ident, unchecked) => { let _x = $got; };

        // Generate a block of code that validates tokens in the form of:
        //
        //      key => ["table", "names"]
        //
        // Matches the partition key / tables names observed by the mock.
        (@assert_writes, $got:ident, $($partition_key:expr => $want_tables:expr, )*) => {
            // Construct the desired writes, keyed by partition key
            #[allow(unused_mut)]
            let mut want_writes: HashMap<PartitionKey, _> = Default::default();
            $(
                let mut want: Vec<String> = $want_tables.into_iter()
                    .map(|t| t.to_string())
                    .collect();
                want.sort();
                want_writes.insert(PartitionKey::from($partition_key), want);
            )*

            pretty_assertions::assert_eq!(want_writes, $got);
        };
    }

    test_write!(
        single_partition,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 2\n\
            another,tag1=A,tag2=B value=42i 3\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1\n\
        ",
        want_writes = [
            "1970-01-01" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Ok(_)
    );

    test_write!(
        multiple_partitions,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        want_writes = [
            "1970-01-01" => ["bananas"],
            "2016-06-13" => ["platanos", "another"],
            "2022-02-08" => ["table"],
        ],
        want_handler_ret = Ok(_)
    );

    test_write!(
        multiple_partitions_upserted,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            platanos,tag1=A,tag2=B value=42i 1\n\
            bananas,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B value=42i 1465839830100400200\n\
        ",
        want_writes = [
            "1970-01-01" => ["bananas", "platanos"],
            "2016-06-13" => ["bananas", "platanos"],
        ],
        want_handler_ret = Ok(_)
    );

    test_write!(
        use_case_backfill_forwards,
        lp = [
            "bananas,tag1=A,tag2=B val=42i 1560433177000000000", // 2019-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1592055577000000000", // 2020-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1623591577000000000", // 2021-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1655127577000000000", // 2022-06-13T13:39:37Z
            // Same as above, different table
            "platanos,tag2=wat val=42i 1560433177000000000", // 2019-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1592055577000000000", // 2020-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1623591577000000000", // 2021-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1655127577000000000", // 2022-06-13T13:39:37Z
        ].join("\n").as_str(),
        want_writes = [
            "2022-06-13" => ["bananas", "platanos"],
            "2021-06-13" => ["bananas", "platanos"],
            "2020-06-13" => ["bananas", "platanos"],
            "2019-06-13" => ["bananas", "platanos"],
        ],
        want_handler_ret = Ok(_)
    );

    test_write!(
        use_case_backfill_backwards,
        lp = [
            "bananas,tag1=A,tag2=B val=42i 1655127577000000000", // 2022-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1623591577000000000", // 2021-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1592055577000000000", // 2020-06-13T13:39:37Z
            "bananas,tag1=A,tag2=B val=42i 1560433177000000000", // 2019-06-13T13:39:37Z
            // Same as above, different table
            "platanos,tag2=wat val=42i 1655127577000000000", // 2022-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1623591577000000000", // 2021-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1592055577000000000", // 2020-06-13T13:39:37Z
            "platanos,tag2=wat val=42i 1560433177000000000", // 2019-06-13T13:39:37Z
        ].join("\n").as_str(),
        want_writes = [
            "2022-06-13" => ["bananas", "platanos"],
            "2021-06-13" => ["bananas", "platanos"],
            "2020-06-13" => ["bananas", "platanos"],
            "2019-06-13" => ["bananas", "platanos"],
        ],
        want_handler_ret = Ok(_)
    );

    #[tokio::test]
    async fn test_write_table_partition_template() {
        let partitioner = Partitioner::default();
        let ns = NamespaceName::new("bananas").expect("valid db name");

        let namespace_schema = namespace_schema(42);

        let bananas_table_template = test_table_partition_override(vec![
            TemplatePart::TagValue("oranges"),
            TemplatePart::TimeFormat("%Y-%m"),
            TemplatePart::TagValue("tag2"),
        ]);

        let lp = "
            bananas,tag1=A,tag2=C val=42i 1\n\
            platanos,tag1=B,tag2=C value=42i 1465839830100400200\n\
            platanos,tag1=A,tag2=D value=42i 1\n\
            bananas,tag1=B,tag2=D value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=D value=42i 1465839830100400200\n\
        ";

        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");

        let writes = writes
            .into_iter()
            .enumerate()
            .map(|(i, (name, data))| {
                let table_partition_template = match name.as_str() {
                    "bananas" => bananas_table_template.clone(),
                    _ => Default::default(),
                };
                (TableId::new(i as _), (name, table_partition_template, data))
            })
            .collect();

        let handler_ret = partitioner.write(&ns, namespace_schema, writes, None).await;

        // Check the partition -> table mapping.
        let got = handler_ret
            .unwrap_or_default()
            .into_iter()
            .map(|partition| {
                // Extract the table names in this partition
                let mut tables = partition
                    .payload
                    .values()
                    .map(|v| v.0.clone())
                    .collect::<Vec<String>>();

                tables.sort();

                (partition.key, tables)
            })
            .collect::<HashMap<_, _>>();

        let expected = HashMap::from([
            (PartitionKey::from("!|1970-01|C"), vec!["bananas".into()]),
            (PartitionKey::from("!|2016-06|D"), vec!["bananas".into()]),
            // This table does not have a partition template override
            (PartitionKey::from("1970-01-01"), vec!["platanos".into()]),
            (PartitionKey::from("2016-06-13"), vec!["platanos".into()]),
        ]);

        pretty_assertions::assert_eq!(expected, got);
    }
}
