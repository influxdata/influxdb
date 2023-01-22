use std::sync::Arc;

use data_types::CompactionLevel;
use futures::StreamExt;
use observability_deps::tracing::info;

use crate::{
    components::{
        compact::{compact_files::compact_files, partition::PartitionInfo},
        Components,
    },
    config::Config,
};

// TODO: modify this comments accordingly as we go
// Currently, we only compact files of level_n with level_n+1 and produce level_n+1 files,
// and with the strictly design that:
//    . Level-0 files can overlap with any files.
//    . Level-N files (N > 0) cannot overlap with any files in the same level.
//    . For Level-0 files, we always pick the smaller `created_at` files to compact (with
//      each other and overlapped L1 files) first.
//    . Level-N+1 files are results of compacting Level-N and/or Level-N+1 files, their `created_at`
//      can be after the `created_at` of other Level-N files but they may include data loaded before
//      the other Level-N files. Hence we should never use `created_at` of Level-N+1 files to order
//      them with Level-N files.
//    . We can only compact different sets of files of the same partition concurrently into the same target_level.
pub async fn compact(config: &Config, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    // TODO: https://github.com/influxdata/influxdb_iox/issues/6657
    // either here or before invoking this function to ignore this partition if it is in skipped_compactions

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let components = Arc::clone(components);

            async move {
                let files = components.partition_files_source.fetch(partition_id).await;
                let files = components.files_filter.apply(files);

                if !components.partition_filter.apply(&files) {
                    return;
                }

                // TODO: only read partition, table and its schema info the first time and cache them
                // Get info for the partition
                let partition = components.partitions_source.fetch_by_id(partition_id).await;
                if partition.is_none() {
                    // Since we retry reading the catalog and cannot find enough needed info,
                    // this partition won't be able to get compacted
                    components
                        .partition_error_sink
                        .record(partition_id, "Cannot find partition info")
                        .await;
                    return;
                }
                let partition = partition.unwrap();

                let table = components.tables_source.fetch(files[0].table_id).await;
                if table.is_none() {
                    components
                        .partition_error_sink
                        .record(partition_id, "Cannot find table ")
                        .await;
                    return;
                }
                let table = table.unwrap();

                // TODO: after we have catalog funciton to read table schema, we should use it
                // and avoid reading namespace schema
                let namespace = components
                    .namespaces_source
                    .fetch_by_id(table.namespace_id)
                    .await;
                if namespace.is_none() {
                    components
                        .partition_error_sink
                        .record(partition_id, "Cannot find namespace")
                        .await;
                    return;
                }
                let namespace = namespace.unwrap();

                let namespace_schema = components
                    .namespaces_source
                    .fetch_schema_by_id(table.namespace_id)
                    .await;
                if namespace_schema.is_none() {
                    components
                        .partition_error_sink
                        .record(partition_id, "Cannot find namespace schema")
                        .await;
                    return;
                }
                let namespace_schema = namespace_schema.unwrap();

                let table_schema = namespace_schema.tables.get(&table.name);
                if table_schema.is_none() {
                    components
                        .partition_error_sink
                        .record(partition_id, "Cannot find table schema")
                        .await;
                    return;
                }
                let table_schema = table_schema.unwrap();

                let partition_info = PartitionInfo::new(
                    partition_id,
                    table.namespace_id,
                    namespace.name,
                    Arc::new(table),
                    Arc::new(table_schema.clone()),
                    partition.sort_key(),
                    partition.partition_key,
                );

                let files = Arc::new(files);

                // TODO: Need a wraper funtion to:
                //    . split files into L0, L1 and L2
                //    . identify right files for hot/cold compaction
                //    . filter right amount of files
                //    . compact many steps hot/cold (need more thinking)
                match compact_files(
                    Arc::clone(&files),
                    Arc::new(partition_info),
                    Arc::new(config),
                    CompactionLevel::FileNonOverlapped,
                )
                .await
                {
                    Ok(create) => {
                        let delete_ids = files.iter().map(|f| f.id).collect::<Vec<_>>();
                        components.commit.commit(&delete_ids, &create).await;
                    }
                    // TODO: remove this stop-gap
                    Err(crate::components::compact::compact_files::Error::NotImplemented) => {
                        info!("not implemented");
                    }
                    #[allow(unreachable_patterns)] // TODO: remove this when stop-gap is removed
                    Err(e) => {
                        components
                            .partition_error_sink
                            .record(partition_id, &e.to_string())
                            .await;
                    }
                }
            }
        })
        .buffer_unordered(config.partition_concurrency.get())
        .collect::<()>()
        .await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_util::assert_batches_sorted_eq;
    use data_types::CompactionLevel;

    use crate::{
        components::hardcoded::hardcoded_components, driver::compact, test_util::TestSetup,
    };

    #[tokio::test]
    async fn test_compact_no_file() {
        test_helpers::maybe_start_logging();

        // no files
        let setup = TestSetup::new(false).await;

        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());

        // compact
        let config = Arc::clone(&setup.config);
        let components = hardcoded_components(&config);
        compact(&config, &components).await;

        // verify catalog is still empty
        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_compact() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let setup = TestSetup::new(true).await;

        // verify 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ]
        );

        // compact
        let config = Arc::clone(&setup.config);
        let components = hardcoded_components(&config);
        compact(&config, &components).await;

        // verify number of files: 6 files are compacted into 2 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 2);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        println!("{:?}", files_and_levels);
        assert_eq!(
            files_and_levels,
            vec![
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // verify the content of files
        // Compacted smaller file with the later data
        let mut files = setup.list_by_table_not_to_delete().await;
        let file1 = files.pop().unwrap();
        let batches = setup.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 210       |      | OH   | 21   | 1970-01-01T00:00:00.000136Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Compacted larger file with the earlier data
        let file0 = files.pop().unwrap();
        let batches = setup.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            [
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 22        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }
}
