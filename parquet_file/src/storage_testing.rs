/// This is test module of the storage module
// The tests have their own module to avoid cycle "use"

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    //use arrow::ipc::RecordBatch;
    use arrow_util::assert_batches_eq;

    use data_types::partition_metadata::TableSummary;

    use crate::{
        metadata::{read_parquet_metadata_from_file, read_statistics_from_parquet_metadata},
        storage::read_schema_from_parquet_metadata,
        utils::*,
    };

    #[tokio::test]
    async fn test_write_read() {
        ////////////////////
        // Create test data which is also the expected data
        let table: &str = "table1";
        let (record_batches, schema, column_summaries, time_range, num_rows) = make_record_batch();
        let mut table_summary = TableSummary::new(table);
        table_summary.columns = column_summaries.clone();
        let record_batch = record_batches[0].clone(); // Get the first one to compare key-value meta data that would be the same for all batches
        let key_value_metadata = record_batch.schema().metadata().clone();

        ////////////////////
        // Make an OS in memory
        let store = make_object_store();

        ////////////////////
        // Store the data as a chunk and write it to in the object store
        // This test Storage::write_to_object_store
        let chunk = make_chunk_given_record_batch(
            Arc::clone(&store),
            record_batches.clone(),
            schema.clone(),
            table,
            column_summaries.clone(),
            time_range,
        )
        .await;

        ////////////////////
        // Now let read it back
        //
        let (_read_table, parquet_data) = load_parquet_from_store(&chunk, Arc::clone(&store)).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();
        //
        // 1. Check metadata at file level: Everything is correct
        let schema_actual = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        //let schema_expected = chunk.table_schema(&table, Selection::All).unwrap();

        // println!("---- Actual file level key value metadata: {:#?}", schema_actual.as_arrow().metadata().clone());
        // println!("---- Actual schema: {:#?}", schema_actual);
        assert_eq!(schema.clone(), schema_actual);
        assert_eq!(
            key_value_metadata.clone(),
            schema_actual.as_arrow().metadata().clone()
        );

        // 2. Check statistics
        let (table_summary_actual, timestamp_range_actual) =
            read_statistics_from_parquet_metadata(&parquet_metadata, &schema_actual, &table)
                .unwrap();
        // println!("Table Summary: {:#?}", table_summary_actual);
        assert_eq!(table_summary_actual, table_summary);
        assert_eq!(timestamp_range_actual, Some(time_range));

        // 3. Check data
        // Read the parquet data from object store
        let (_read_table, parquet_data) = load_parquet_from_store(&chunk, Arc::clone(&store)).await;
        // Note that the read_data_from_parquet_data function fixes the row-group/batches' level metadata bug in arrow
        let actual_record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);
        let mut actual_num_rows = 0;
        for batch in actual_record_batches.clone() {
            actual_num_rows += batch.num_rows();

            // Check if record batch has meta data
            // println!("Record batch: {:#?}", batch);
            let batch_key_value_metadata = batch.schema().metadata().clone();
            // println!("Batch key value meta data: {:#?}", batch_key_value_metadata); // should have value
            assert_eq!(
                schema.as_arrow().metadata().clone(),
                batch_key_value_metadata
            );
        }

        // Now verify return results. This assert_batches_eq still works correctly without the metadata
        // We might modify it to make it include checking metadata
        let expected = vec![
            "+--------------+-----------+-----------------------+--------------------+------------------+----------------------+------------------+------------------+---------------+----------------+------------+----------------------------+",
            "| tag_nonempty | tag_empty | field_string_nonempty | field_string_empty | field_i64_normal | field_i64_range      | field_u64_normal | field_f64_normal | field_f64_inf | field_f64_zero | field_bool | time                       |",
            "+--------------+-----------+-----------------------+--------------------+------------------+----------------------+------------------+------------------+---------------+----------------+------------+----------------------------+",
            "| foo          |           | foo                   |                    | -1               | -9223372036854775808 | 1                | 10.1             | 0             | 0              | true       | 1970-01-01 00:00:00.000001 |",
            "| bar          |           | bar                   |                    | 2                | 9223372036854775807  | 2                | 20.1             | inf           | 0              | false      | 1970-01-01 00:00:00.000002 |",
            "| baz          |           | baz                   |                    | 3                | -9223372036854775808 | 3                | 30.1             | -inf          | 0              | true       | 1970-01-01 00:00:00.000003 |",
            "| foo          |           | foo                   |                    | 4                | 9223372036854775807  | 4                | 40.1             | 1             | 0              | false      | 1970-01-01 00:00:00.000004 |",
            "+--------------+-----------+-----------------------+--------------------+------------------+----------------------+------------------+------------------+---------------+----------------+------------+----------------------------+",
        ];
        assert_eq!(num_rows, actual_num_rows);
        assert_batches_eq!(expected.clone(), &record_batches);
        assert_batches_eq!(expected, &actual_record_batches);
    }
}
