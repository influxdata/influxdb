/// This is test module of the storage module
// The tests have their own module to avoid cycle "use"

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_util::assert_batches_eq;

    use data_types::partition_metadata::TableSummary;

    use crate::{
        metadata::IoxParquetMetaData,
        test_utils::{
            chunk_addr, load_parquet_from_store, make_chunk_given_record_batch,
            make_iox_object_store, make_record_batch, read_data_from_parquet_data,
        },
    };

    #[tokio::test]
    async fn test_write_read() {
        ////////////////////
        // Create test data which is also the expected data
        let addr = chunk_addr(1);
        let table = Arc::clone(&addr.table_name);
        let (record_batches, schema, column_summaries, num_rows) = make_record_batch("foo");
        let mut table_summary = TableSummary::new(table.to_string());
        table_summary.columns = column_summaries.clone();
        let record_batch = record_batches[0].clone(); // Get the first one to compare key-value meta data that would be the same for all batches
        let key_value_metadata = record_batch.schema().metadata().clone();

        ////////////////////
        // Make an OS in memory
        let store = make_iox_object_store().await;

        ////////////////////
        // Store the data as a chunk and write it to in the object store
        // This test Storage::write_to_object_store
        let chunk = make_chunk_given_record_batch(
            Arc::clone(&store),
            record_batches.clone(),
            schema.clone(),
            addr,
            column_summaries.clone(),
        )
        .await;

        ////////////////////
        // Now let read it back
        //
        let parquet_data = load_parquet_from_store(&chunk, Arc::clone(&store))
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        let decoded = parquet_metadata.decode().unwrap();
        //
        // 1. Check metadata at file level: Everything is correct
        let schema_actual = decoded.read_schema().unwrap();
        assert_eq!(Arc::new(schema.clone()), schema_actual);
        assert_eq!(
            key_value_metadata.clone(),
            schema_actual.as_arrow().metadata().clone()
        );

        // 2. Check statistics
        let table_summary_actual = decoded.read_statistics(&schema_actual).unwrap();
        assert_eq!(table_summary_actual, table_summary.columns);

        // 3. Check data
        // Note that the read_data_from_parquet_data function fixes the row-group/batches' level metadata bug in arrow
        let actual_record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);
        let mut actual_num_rows = 0;
        for batch in actual_record_batches.clone() {
            actual_num_rows += batch.num_rows();

            // Check if record batch has meta data
            let batch_key_value_metadata = batch.schema().metadata().clone();
            assert_eq!(
                schema.as_arrow().metadata().clone(),
                batch_key_value_metadata
            );
        }

        // Now verify return results. This assert_batches_eq still works correctly without the metadata
        // We might modify it to make it include checking metadata or add a new comparison checking macro that prints out the metadata too
        let expected = vec![
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
            "| foo_tag_normal | foo_tag_empty | foo_tag_null_some | foo_tag_null_all | foo_field_string_normal | foo_field_string_empty | foo_field_string_null_some | foo_field_string_null_all | foo_field_i64_normal | foo_field_i64_range  | foo_field_i64_null_some | foo_field_i64_null_all | foo_field_u64_normal | foo_field_u64_range  | foo_field_u64_null_some | foo_field_u64_null_all | foo_field_f64_normal | foo_field_f64_inf | foo_field_f64_zero | foo_field_f64_nan_some | foo_field_f64_nan_all | foo_field_f64_null_some | foo_field_f64_null_all | foo_field_bool_normal | foo_field_bool_null_some | foo_field_bool_null_all | time                        |",
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
            "| foo            |               |                   |                  | foo                     |                        |                            |                           | -1                   | -9223372036854775808 |                         |                        | 1                    | 0                    |                         |                        | 10.1                 | 0                 | 0                  | NaN                    | NaN                   |                         |                        | true                  |                          |                         | 1970-01-01T00:00:00.000001Z |",
            "| bar            |               | bar               |                  | bar                     |                        | bar                        |                           | 2                    | 9223372036854775807  | 2                       |                        | 2                    | 18446744073709551615 | 2                       |                        | 20.1                 | inf               | -0                 | 2                      | NaN                   | 20.1                    |                        | false                 | false                    |                         | 1970-01-01T00:00:00.000002Z |",
            "| baz            |               | baz               |                  | baz                     |                        | baz                        |                           | 3                    | -9223372036854775808 | 3                       |                        | 3                    | 0                    | 3                       |                        | 30.1                 | -inf              | 0                  | 1                      | NaN                   | 30.1                    |                        | true                  | true                     |                         | 1970-01-01T00:00:00.000003Z |",
            "| foo            |               |                   |                  | foo                     |                        |                            |                           | 4                    | 9223372036854775807  |                         |                        | 4                    | 18446744073709551615 |                         |                        | 40.1                 | 1                 | -0                 | NaN                    | NaN                   |                         |                        | false                 |                          |                         | 1970-01-01T00:00:00.000004Z |",
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
        ];
        assert_eq!(num_rows, actual_num_rows);
        assert_batches_eq!(expected.clone(), &record_batches);
        assert_batches_eq!(expected, &actual_record_batches);
    }
}
