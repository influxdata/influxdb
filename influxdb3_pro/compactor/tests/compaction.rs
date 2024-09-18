use arrow::array::BooleanArray;
use arrow::array::DictionaryArray;
use arrow::array::Float64Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::SchemaRef;
use data_types::NamespaceName;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_pro_compactor::{compact_files, CompactFilesArgs, CompactorOutput};
use influxdb3_pro_data_layout::{CompactionSequenceNumber, GenerationLevel};
use influxdb3_wal::WalConfig;
use influxdb3_write::last_cache::LastCacheProvider;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::Bufferer;
use iox_query::exec::DedicatedExecutor;
use iox_query::exec::Executor;
use iox_query::exec::ExecutorConfig;
use iox_time::MockProvider;
use iox_time::Time;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet_file::storage::ParquetStorage;
use parquet_file::storage::StorageId;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[tokio::test]
async fn five_files_multiple_series_same_schema() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());

    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate the schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "test_table,id=0i field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );

    let batch_maker = BatchMaker::new(Arc::clone(&schema));
    let batch1 =
        batch_maker.id_field_time(["a", "b", "c", "d", "e"], [0, 0, 0, 0, 0], [1, 2, 3, 4, 5]);
    let batch2 = batch_maker.id_field_time(
        ["e", "e", "e", "f", "g", "h", "i", "j"],
        [0, 0, 0, 0, 0, 0, 0, 0],
        [5, 6, 7, 6, 7, 8, 9, 10],
    );
    let batch3 = batch_maker.id_field_time(
        ["e", "f", "g", "h", "i", "j", "k"],
        [0, 0, 0, 0, 0, 0, 0],
        [5, 6, 7, 8, 9, 10, 11],
    );

    // Test with only a single series
    let batch4 = batch_maker.id_field_time(
        ["e", "e", "e", "e", "e", "e", "e"],
        [0, 0, 0, 0, 0, 0, 0],
        [0, 1, 2, 3, 4, 5, 6],
    );

    // Test for a half full file
    let batch5 = batch_maker.id_field_time(["l"], [0], [0]);

    // write to files
    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;
    let path2 = test_writer.write("test/batch/2", batch2).await;
    let path3 = test_writer.write("test/batch/3", batch3).await;
    let path4 = test_writer.write("test/batch/4", batch4).await;
    let path5 = test_writer.write("test/batch/5", batch5).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: vec!["id".into()],
        paths: vec![path1, path2, path3, path4, path5],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: vec!["id".into(), "field".into()],
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput {
        output_paths,
        file_index,
        ..
    } = compact_files(args).await.unwrap();

    // Expect series to be split evenly across the files, no series should be
    // split across files
    let file_contents = files_to_string(&obj_store, &output_paths).await;
    insta::assert_snapshot!(file_contents, @r###"
    ********
    File: 0
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | a  | 1970-01-01T00:00:00.000000001 |
    | 0     | b  | 1970-01-01T00:00:00.000000002 |
    +-------+----+-------------------------------+
    ********
    File: 1
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | c  | 1970-01-01T00:00:00.000000003 |
    | 0     | d  | 1970-01-01T00:00:00.000000004 |
    +-------+----+-------------------------------+
    ********
    File: 2
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | e  | 1970-01-01T00:00:00           |
    | 0     | e  | 1970-01-01T00:00:00.000000001 |
    | 0     | e  | 1970-01-01T00:00:00.000000002 |
    | 0     | e  | 1970-01-01T00:00:00.000000003 |
    | 0     | e  | 1970-01-01T00:00:00.000000004 |
    | 0     | e  | 1970-01-01T00:00:00.000000005 |
    | 0     | e  | 1970-01-01T00:00:00.000000006 |
    | 0     | e  | 1970-01-01T00:00:00.000000007 |
    +-------+----+-------------------------------+
    ********
    File: 3
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | f  | 1970-01-01T00:00:00.000000006 |
    | 0     | g  | 1970-01-01T00:00:00.000000007 |
    +-------+----+-------------------------------+
    ********
    File: 4
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | h  | 1970-01-01T00:00:00.000000008 |
    | 0     | i  | 1970-01-01T00:00:00.000000009 |
    +-------+----+-------------------------------+
    ********
    File: 5
    ********
    -------
    Batch
    -------
    +-------+----+-------------------------------+
    | field | id | time                          |
    +-------+----+-------------------------------+
    | 0     | j  | 1970-01-01T00:00:00.000000010 |
    | 0     | k  | 1970-01-01T00:00:00.000000011 |
    +-------+----+-------------------------------+
    ********
    File: 6
    ********
    -------
    Batch
    -------
    +-------+----+---------------------+
    | field | id | time                |
    +-------+----+---------------------+
    | 0     | l  | 1970-01-01T00:00:00 |
    +-------+----+---------------------+
    "###);

    // Index assertions
    assert_eq!(file_index.lookup("field".into(), "0".into()).len(), 7);
    assert_eq!(file_index.lookup("field".into(), "3".into()).len(), 0);
    assert_eq!(file_index.lookup("id".into(), "a".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "b".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "c".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "d".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "e".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "f".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "g".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "h".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "i".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "j".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "k".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "l".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "m".into()).len(), 0);
}

#[tokio::test]
async fn two_files_two_series_and_same_schema() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());

    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate the schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "test_table,id=0i,host=\"foo\" field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );

    let batch_maker = BatchMaker::new(Arc::clone(&schema));

    let batch1 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [2, 2, 2, 2, 2, 2],
        [1, 2, 3, 4, 5, 6],
    );

    let batch2 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b"],
        [7, 6, 5, 4, 3, 1, 8, 9, 10, 11, 12, 13],
        [1, 2, 3, 4, 5, 7, 1, 2, 3, 4, 5, 6],
    );

    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;
    let path2 = test_writer.write("test/batch/2", batch2).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: vec!["id".into(), "host".into()],
        paths: vec![path2, path1],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: vec!["id".into(), "host".into(), "field".into()],
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput {
        output_paths,
        file_index,
        ..
    } = compact_files(args).await.unwrap();

    // Verify contents.
    let file_contents = files_to_string(&obj_store, &output_paths).await;
    insta::assert_snapshot!(file_contents, @r###"
    ********
    File: 0
    ********
    -------
    Batch
    -------
    +-------+------+----+-------------------------------+
    | field | host | id | time                          |
    +-------+------+----+-------------------------------+
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000001 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000002 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000003 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000004 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000005 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000006 |
    | 1     | a    | 1  | 1970-01-01T00:00:00.000000007 |
    +-------+------+----+-------------------------------+
    ********
    File: 1
    ********
    -------
    Batch
    -------
    +-------+------+----+-------------------------------+
    | field | host | id | time                          |
    +-------+------+----+-------------------------------+
    | 8     | b    | 1  | 1970-01-01T00:00:00.000000001 |
    | 9     | b    | 1  | 1970-01-01T00:00:00.000000002 |
    | 10    | b    | 1  | 1970-01-01T00:00:00.000000003 |
    | 11    | b    | 1  | 1970-01-01T00:00:00.000000004 |
    | 12    | b    | 1  | 1970-01-01T00:00:00.000000005 |
    | 13    | b    | 1  | 1970-01-01T00:00:00.000000006 |
    +-------+------+----+-------------------------------+
    "###);
    // Index assertions
    assert_eq!(file_index.lookup("field".into(), "1".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "2".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "3".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "4".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "5".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "6".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "7".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "8".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "9".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "10".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "11".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "12".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "13".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "14".into()).len(), 0);
    assert_eq!(file_index.lookup("id".into(), "1".into()).len(), 2);
    assert_eq!(file_index.lookup("id".into(), "2".into()).len(), 0);
}

#[tokio::test]
async fn two_files_same_series_and_schema() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());

    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate the schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "test_table,id=0i,host=\"foo\" field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );

    let batch_maker = BatchMaker::new(Arc::clone(&schema));

    let batch1 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [2, 2, 2, 2, 2, 2],
        [1, 2, 3, 4, 5, 6],
    );

    let batch2 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [7, 6, 5, 4, 3, 1],
        [1, 2, 3, 4, 5, 7],
    );

    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;
    let path2 = test_writer.write("test/batch/2", batch2).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: vec!["id".into(), "host".into()],
        paths: vec![path1, path2],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: vec!["id".into(), "host".into(), "field".into()],
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput {
        output_paths,
        file_index,
        ..
    } = compact_files(args).await.unwrap();

    // Read those files into memory to be checked for validity
    //
    // TODO AAL: I think this test should have a single file, not two
    let file_contents = files_to_string(&obj_store, &output_paths).await;
    insta::assert_snapshot!(file_contents, @r###"
    ********
    File: 0
    ********
    -------
    Batch
    -------
    +-------+------+----+-------------------------------+
    | field | host | id | time                          |
    +-------+------+----+-------------------------------+
    | 7     | a    | 1  | 1970-01-01T00:00:00.000000001 |
    | 6     | a    | 1  | 1970-01-01T00:00:00.000000002 |
    | 5     | a    | 1  | 1970-01-01T00:00:00.000000003 |
    | 4     | a    | 1  | 1970-01-01T00:00:00.000000004 |
    | 3     | a    | 1  | 1970-01-01T00:00:00.000000005 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000006 |
    | 1     | a    | 1  | 1970-01-01T00:00:00.000000007 |
    +-------+------+----+-------------------------------+
    "###
    );
    // Index assertions
    assert_eq!(file_index.lookup("field".into(), "1".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "2".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "3".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "4".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "5".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "6".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "7".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "8".into()).len(), 0);
    assert_eq!(file_index.lookup("id".into(), "1".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "2".into()).len(), 0);
    assert_eq!(file_index.lookup("host".into(), "a".into()).len(), 1);
    assert_eq!(file_index.lookup("host".into(), "b".into()).len(), 0);
}
#[tokio::test]
async fn two_files_similar_series_and_compatible_schema() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate each schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "other_test_table,id=0i,host=\"foo\" field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "test_table,id=0i,host=\"foo\",extra_tag=0i field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema1 = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("other_test_table")
            .unwrap()
            .as_arrow(),
    );

    let schema2 = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );
    let batch_maker1 = BatchMaker::new(Arc::clone(&schema1));
    let batch_maker2 = BatchMaker::new(Arc::clone(&schema2));

    let batch1 = batch_maker1.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [2, 2, 2, 2, 2, 2],
        [1, 2, 3, 4, 5, 6],
    );

    let batch2 = batch_maker2.id_host_extra_tag_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        ["5", "5", "5", "5", "5", "5"],
        [7, 6, 5, 4, 3, 1],
        [1, 2, 3, 4, 5, 7],
    );

    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;
    let path2 = test_writer.write("test/batch/2", batch2).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: vec!["id".into(), "host".into(), "extra_tag".into()],
        paths: vec![path1, path2],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: vec![
            "id".into(),
            "host".into(),
            "field".into(),
            "extra_tag".into(),
        ],
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput {
        output_paths,
        file_index,
        ..
    } = compact_files(args).await.unwrap();

    // Read those files into memory to be checked for validity
    //
    // TODO AAL: I think this test should have 2 files, not three
    let file_contents = files_to_string(&obj_store, &output_paths).await;
    insta::assert_snapshot!(file_contents, @r###"
    ********
    File: 0
    ********
    -------
    Batch
    -------
    +-----------+-------+------+----+-------------------------------+
    | extra_tag | field | host | id | time                          |
    +-----------+-------+------+----+-------------------------------+
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000001 |
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000002 |
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000003 |
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000004 |
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000005 |
    |           | 2     | a    | 1  | 1970-01-01T00:00:00.000000006 |
    +-----------+-------+------+----+-------------------------------+
    ********
    File: 1
    ********
    -------
    Batch
    -------
    +-----------+-------+------+----+-------------------------------+
    | extra_tag | field | host | id | time                          |
    +-----------+-------+------+----+-------------------------------+
    | 5         | 7     | a    | 1  | 1970-01-01T00:00:00.000000001 |
    | 5         | 6     | a    | 1  | 1970-01-01T00:00:00.000000002 |
    | 5         | 5     | a    | 1  | 1970-01-01T00:00:00.000000003 |
    | 5         | 4     | a    | 1  | 1970-01-01T00:00:00.000000004 |
    | 5         | 3     | a    | 1  | 1970-01-01T00:00:00.000000005 |
    | 5         | 1     | a    | 1  | 1970-01-01T00:00:00.000000007 |
    +-----------+-------+------+----+-------------------------------+
    "###
    );
    // Index assertions
    assert_eq!(
        file_index.lookup("extra_tag".into(), "null".into()).len(),
        1
    );
    assert_eq!(file_index.lookup("extra_tag".into(), "5".into()).len(), 1);
    assert_eq!(file_index.lookup("extra_tag".into(), "6".into()).len(), 0);
    assert_eq!(file_index.lookup("field".into(), "1".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "2".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "3".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "4".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "5".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "6".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "7".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "8".into()).len(), 0);
    assert_eq!(file_index.lookup("id".into(), "1".into()).len(), 2);
    assert_eq!(file_index.lookup("id".into(), "2".into()).len(), 0);
    assert_eq!(file_index.lookup("host".into(), "a".into()).len(), 2);
    assert_eq!(file_index.lookup("host".into(), "b".into()).len(), 0);
}

/// Makes sure that sort and dedupe works as expected
#[tokio::test]
async fn deduplication_of_data() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate the schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            "test_table,id=0i,host=\"foo\" field=0i 0\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );

    let batch_maker = BatchMaker::new(Arc::clone(&schema));

    let batch1 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [2, 2, 2, 2, 2, 2],
        [1, 2, 3, 4, 5, 6],
    );

    let batch2 = batch_maker.id_host_field_time(
        ["1", "1", "1", "1", "1", "1"],
        ["a", "a", "a", "a", "a", "a"],
        [3, 3, 3, 3, 3, 3],
        [1, 2, 3, 4, 5, 7],
    );

    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;
    let path2 = test_writer.write("test/batch/2", batch2).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: vec!["id".into(), "host".into()],
        paths: vec![path2, path1],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: vec!["id".into(), "host".into(), "field".into()],
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput {
        output_paths,
        file_index,
        ..
    } = compact_files(args).await.unwrap();

    // Read those files into memory to be checked for validity
    let file_contents = files_to_string(&obj_store, &output_paths).await;
    insta::assert_snapshot!(file_contents, @r###"
    ********
    File: 0
    ********
    -------
    Batch
    -------
    +-------+------+----+-------------------------------+
    | field | host | id | time                          |
    +-------+------+----+-------------------------------+
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000001 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000002 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000003 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000004 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000005 |
    | 2     | a    | 1  | 1970-01-01T00:00:00.000000006 |
    | 3     | a    | 1  | 1970-01-01T00:00:00.000000007 |
    +-------+------+----+-------------------------------+
    "###
    );
    // Index Assertions
    assert_eq!(file_index.lookup("field".into(), "2".into()).len(), 1);
    assert_eq!(file_index.lookup("field".into(), "3".into()).len(), 1);
    assert_eq!(file_index.lookup("host".into(), "a".into()).len(), 1);
    assert_eq!(file_index.lookup("host".into(), "b".into()).len(), 0);
    assert_eq!(file_index.lookup("id".into(), "1".into()).len(), 1);
    assert_eq!(file_index.lookup("id".into(), "2".into()).len(), 0);
}

/// Test to determine if we can cast everything in our current data model in our file index
#[tokio::test]
async fn compactor_casting() {
    // Create and write multiple different files to the Object Store
    let obj_store = Arc::new(InMemory::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
        "test-host",
    ));
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(Catalog::new()),
            Arc::new(LastCacheProvider::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(Executor::new_testing()),
            WalConfig::test_config(),
        )
        .await
        .unwrap(),
    );

    // write into the buffer to recreate the schema and to create the tables and DB
    // for the compactor test
    write_buffer
        .write_lp(
            NamespaceName::new("test_db").unwrap(),
            // Schema for:
            // - Tags (Dictionary<i32, Utf8>)
            // - Utf8
            // - Float64
            // - UInt64
            // - Int64
            // - Boolean
            // - Timestamp<Nanos>
            "test_table,a=0i,b=\"foo\",c=1.0,d=2,e=true f=3i,g=4.0,h=false,i=\"bar\" 100\n",
            Time::from_timestamp_nanos(0),
            false,
            influxdb3_write::Precision::Nanosecond,
        )
        .await
        .unwrap();

    let schema = Arc::new(
        write_buffer
            .catalog()
            .db_schema("test_db")
            .unwrap()
            .get_table_schema("test_table")
            .unwrap()
            .as_arrow(),
    );
    let a: DictionaryArray<Int32Type> = ["0"].into_iter().collect();
    let b: DictionaryArray<Int32Type> = ["foo"].into_iter().collect();
    let c: DictionaryArray<Int32Type> = ["1.0"].into_iter().collect();
    let d: DictionaryArray<Int32Type> = ["2"].into_iter().collect();
    let e: DictionaryArray<Int32Type> = ["true"].into_iter().collect();
    let f: Int64Array = [3i64].into_iter().collect();
    let g: Float64Array = [4.0].into_iter().collect();
    let h: BooleanArray = [Some(false)].into_iter().collect();
    let i = StringArray::from(vec!["bar"]);
    let j = TimestampNanosecondArray::from(vec![100]);

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // NB different order than arguments to function
            Arc::new(a),
            Arc::new(b),
            Arc::new(c),
            Arc::new(d),
            Arc::new(e),
            Arc::new(f),
            Arc::new(g),
            Arc::new(h),
            Arc::new(i),
            Arc::new(j.clone()),
        ],
    )
    .unwrap();

    let test_writer = TestFileWriter::new(Arc::clone(&obj_store));

    let path1 = test_writer.write("test/batch/1", batch1).await;

    let db_schema = write_buffer.catalog().db_schema("test_db").unwrap();
    let args = CompactFilesArgs {
        compactor_id: "compactor_1".into(),
        compaction_sequence_number: CompactionSequenceNumber::new(1),
        db_schema,
        table_name: "test_table".into(),
        sort_keys: ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        paths: vec![path1],
        limit: 2,
        generation: GenerationLevel::two(),
        index_columns: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "time"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        object_store: persister.object_store(),
        object_store_url: persister.object_store_url().clone(),
        exec: make_exec(Arc::clone(&obj_store) as Arc<dyn ObjectStore>),
    };
    let CompactorOutput { file_index, .. } = compact_files(args).await.unwrap();

    // Index Assertions
    // b=\"foo\",c=1.0,d=2,e=true f=3i,g=4.0,h=false,i=\"bar\" 100\n",
    assert_eq!(file_index.lookup("a".into(), "0".into()).len(), 1);
    assert_eq!(file_index.lookup("b".into(), "foo".into()).len(), 1);
    assert_eq!(file_index.lookup("c".into(), "1.0".into()).len(), 1);
    assert_eq!(file_index.lookup("d".into(), "2".into()).len(), 1);
    assert_eq!(file_index.lookup("e".into(), "true".into()).len(), 1);
    assert_eq!(file_index.lookup("f".into(), "3".into()).len(), 1);
    assert_eq!(file_index.lookup("g".into(), "4.0".into()).len(), 1);
    assert_eq!(file_index.lookup("h".into(), "false".into()).len(), 1);
    assert_eq!(file_index.lookup("i".into(), "bar".into()).len(), 1);
    assert_eq!(
        file_index
            .lookup("time".into(), "1970-01-01T00:00:00.000000100".into())
            .len(),
        1
    );
}

fn make_exec(object_store: Arc<dyn ObjectStore>) -> Arc<Executor> {
    let metrics = Arc::new(metric::Registry::default());

    let parquet_store = ParquetStorage::new(
        Arc::clone(&object_store),
        StorageId::from("test_exec_storage"),
    );
    Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            // Default to 1gb
            mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
        },
        DedicatedExecutor::new_testing(),
    ))
}

/// Creates batches for testing from a given schema
#[derive(Debug)]
struct BatchMaker {
    schema: SchemaRef,
}

impl BatchMaker {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    /// returns a new `RecordBatch` with three columns with the specified values
    ///
    /// * id DictionaryArray
    /// * field Int64Array
    /// * time TimestampNanosecondArray
    fn id_field_time<'a>(
        &self,
        id: impl IntoIterator<Item = &'a str>,
        field: impl IntoIterator<Item = i64>,
        time: impl IntoIterator<Item = i64>,
    ) -> RecordBatch {
        let id_array: DictionaryArray<Int32Type> = id.into_iter().collect();
        let field_array: Int64Array = field.into_iter().collect();
        let time = time.into_iter().collect::<Vec<_>>();
        let time_array = TimestampNanosecondArray::from(time);
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                // NB different order than arguments to function
                Arc::new(field_array),
                Arc::new(id_array),
                Arc::new(time_array),
            ],
        )
        .unwrap()
    }

    /// returns a new `RecordBatch` with four columns with the specified values
    ///
    /// * id DictionaryArray
    /// * host DictionaryArray
    /// * field Int64Array
    /// * time TimestampNanosecondArray
    fn id_host_field_time<'a>(
        &self,
        id: impl IntoIterator<Item = &'a str>,
        host: impl IntoIterator<Item = &'a str>,
        field: impl IntoIterator<Item = i64>,
        time: impl IntoIterator<Item = i64>,
    ) -> RecordBatch {
        let id_array: DictionaryArray<Int32Type> = id.into_iter().collect();
        let host_array: DictionaryArray<Int32Type> = host.into_iter().collect();
        let field_array: Int64Array = field.into_iter().collect();
        let time = time.into_iter().collect::<Vec<_>>();
        let time_array = TimestampNanosecondArray::from(time);
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                // NB different order than arguments to function
                Arc::new(field_array),
                Arc::new(host_array),
                Arc::new(id_array),
                Arc::new(time_array),
            ],
        )
        .unwrap()
    }

    /// returns a new `RecordBatch` with four columns with the specified values
    ///
    /// * id DictionaryArray
    /// * host DictionaryArray
    /// * extra_tag DictionaryArray
    /// * field Int64Array
    /// * time TimestampNanosecondArray
    fn id_host_extra_tag_field_time<'a>(
        &self,
        id: impl IntoIterator<Item = &'a str>,
        host: impl IntoIterator<Item = &'a str>,
        extra_tag: impl IntoIterator<Item = &'a str>,
        field: impl IntoIterator<Item = i64>,
        time: impl IntoIterator<Item = i64>,
    ) -> RecordBatch {
        let id_array: DictionaryArray<Int32Type> = id.into_iter().collect();
        let host_array: DictionaryArray<Int32Type> = host.into_iter().collect();
        let extra_tag_array: DictionaryArray<Int32Type> = extra_tag.into_iter().collect();
        let field_array: Int64Array = field.into_iter().collect();
        let time = time.into_iter().collect::<Vec<_>>();
        let time_array = TimestampNanosecondArray::from(time);
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                // NB different order than arguments to function
                Arc::new(extra_tag_array),
                Arc::new(field_array),
                Arc::new(host_array),
                Arc::new(id_array),
                Arc::new(time_array),
            ],
        )
        .unwrap()
    }
}

/// writes batches to test parquet files
struct TestFileWriter {
    obj_store: Arc<InMemory>,
}

impl TestFileWriter {
    /// creates a new `TestFileWriter` with the specified object store
    fn new(obj_store: Arc<InMemory>) -> Self {
        Self { obj_store }
    }

    /// writes the specified batch to the specified path, returning
    /// the path to the written file
    async fn write(&self, path: &str, batch: RecordBatch) -> ObjPath {
        let schema = batch.schema();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let path = ObjPath::from(path);
        self.obj_store
            .put(&path, buffer.into())
            .await
            .expect("write to object store");

        path
    }
}

/// Read the contents of a set of parquet files into a string for comparison
async fn files_to_string(obj_store: &Arc<InMemory>, paths: &[ObjPath]) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for (idx, path) in paths.iter().enumerate() {
        writeln!(&mut s, "********").unwrap();
        writeln!(&mut s, "File: {idx}").unwrap();
        writeln!(&mut s, "********").unwrap();
        let bytes = obj_store.get(path).await.unwrap().bytes().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();

        // print out the contents of the parquet file, batch by batch
        for batch in reader {
            let batch = batch.unwrap();
            writeln!(&mut s, "-------").unwrap();
            writeln!(&mut s, "Batch").unwrap();
            writeln!(&mut s, "-------").unwrap();
            writeln!(&mut s, "{}", pretty_format_batches(&[batch]).unwrap()).unwrap();
        }
    }
    s
}
