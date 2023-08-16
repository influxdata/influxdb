use std::path::PathBuf;

use arrow::{
    array::as_generic_binary_array,
    datatypes::{DataType, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::FlightError,
    sql::{
        Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo,
    },
    FlightClient, FlightDescriptor, IpcMessage,
};
use arrow_util::test_util::batches_to_sorted_lines;
use assert_cmd::assert::OutputAssertExt;
use assert_matches::assert_matches;
use bytes::Bytes;
use datafusion::common::assert_contains;
use futures::{FutureExt, TryStreamExt};
use influxdb_iox_client::flightsql::FlightSqlClient;
use predicates::prelude::*;
use prost::Message;
use test_helpers_end_to_end::{
    maybe_skip_integration, Authorizer, MiniCluster, Step, StepTest, StepTestState,
};
use tokio::process::Command;

#[tokio::test]
async fn flightsql_adhoc_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = format!("select * from {table_name}");
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.query(sql).await.unwrap();
                    let batches = collect_stream(stream).await;
                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------+------+--------------------------------+-----+
                    - "| tag1 | tag2 | time                           | val |"
                    - +------+------+--------------------------------+-----+
                    - "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |"
                    - "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |"
                    - +------+------+--------------------------------+-----+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_adhoc_query_error() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(
                "foo,tag1=A,tag2=B val=42i 123456\n\
                 foo,tag1=A,tag2=C val=43i 123457"
                    .to_string(),
            ),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = String::from("select * from incorrect_table");

                    let mut client = flightsql_client(state.cluster());

                    let err = client.query(sql).await.unwrap_err();

                    // namespaces are created on write
                    assert_contains!(
                        err.to_string(),
                        "table 'public.iox.incorrect_table' not found"
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_prepared_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = format!("select * from {table_name}");
                    let mut client = flightsql_client(state.cluster());

                    let handle = client.prepare(sql).await.unwrap();
                    let stream = client.execute(handle).await.unwrap();

                    let batches = collect_stream(stream).await;
                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------+------+--------------------------------+-----+
                    - "| tag1 | tag2 | time                           | val |"
                    - +------+------+--------------------------------+-----+
                    - "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |"
                    - "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |"
                    - +------+------+--------------------------------+-----+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_sql_infos() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    // test with no filtering
                    let batches = collect_stream(client.get_sql_info(vec![]).await.unwrap()).await;
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    // 85 `SqlInfo` entries are returned by IOx's GetSqlInfo implementation
                    // if we change what is returned then this number should be updated too
                    assert_eq!(total_rows, 85);

                    // only retrieve requested metadata
                    let infos = vec![
                        SqlInfo::FlightSqlServerName as u32,
                        SqlInfo::FlightSqlServerArrowVersion as u32,
                        SqlInfo::SqlBatchUpdatesSupported as u32,
                        999999, //  model some unknown info requested
                    ];

                    let batches = collect_stream(client.get_sql_info(infos).await.unwrap()).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +-----------+-----------------------------+
                    - "| info_name | value                       |"
                    - +-----------+-----------------------------+
                    - "| 0         | {string_value=InfluxDB IOx} |"
                    - "| 2         | {string_value=1.3}          |"
                    - "| 572       | {bool_value=false}          |"
                    - +-----------+-----------------------------+
                    "###
                    );

                    // Test zero case (nothing matches)
                    let infos = vec![
                        999999, //  model some unknown info requested
                    ];

                    let batches = collect_stream(client.get_sql_info(infos).await.unwrap()).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - ++
                    - ++
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_catalogs() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.get_catalogs().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +--------------+
                    - "| catalog_name |"
                    - +--------------+
                    - "| public       |"
                    - +--------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_catalogs_matches_information_schema() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    // output of get_catalogs  is built manually in
                    // IOx, so it is important it remains in sync with
                    // the actual contents of the information schema
                    let stream = client
                        .get_catalogs()
                        .await
                        .unwrap();
                    let get_catalogs_batches = collect_stream(stream).await;
                    let get_catalogs_output = batches_to_sorted_lines(&get_catalogs_batches);

                    let sql = "SELECT DISTINCT table_catalog AS catalog_name FROM information_schema.tables ORDER BY table_catalog";

                    let stream = client.query(sql).await.unwrap();
                    let information_schema_batches = collect_stream(stream).await;
                    let information_schema_output =
                        batches_to_sorted_lines(&information_schema_batches);

                    insta::assert_yaml_snapshot!(
                        get_catalogs_output,
                        @r###"
                    ---
                    - +--------------+
                    - "| catalog_name |"
                    - +--------------+
                    - "| public       |"
                    - +--------------+
                    "###
                    );

                    assert_eq!(get_catalogs_output, information_schema_output);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_cross_reference() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let primary_table_name = "primary_table";
    let foreign_table_name = "foreign_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{primary_table_name},tag1=A,tag2=B val=42i 123456\n\
                 {primary_table_name},tag1=A,tag2=C val=43i 123457\n
                 {foreign_table_name},tag1=B,tag2=D val=42i 123456\n\
                 {foreign_table_name},tag1=C,tag2=F val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let pk_catalog: Option<String> = None;
                    let pk_db_schema: Option<String> = None;
                    let fk_catalog: Option<String> = None;
                    let fk_db_schema: Option<String> = None;

                    let stream = client
                        .get_cross_reference(
                            pk_catalog,
                            pk_db_schema,
                            primary_table_name.to_string(),
                            fk_catalog,
                            fk_db_schema,
                            foreign_table_name.to_string(),
                        )
                        .await
                        .unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - ++
                    - ++
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_tables() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    struct TestCase {
                        catalog: Option<&'static str>,
                        db_schema_filter_pattern: Option<&'static str>,
                        table_name_filter_pattern: Option<&'static str>,
                        table_types: Vec<String>,
                        include_schema: bool,
                    }
                    let cases = [
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            table_types: vec![],
                            include_schema: false,
                        },
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            table_types: vec!["BASE TABLE".to_string()],
                            include_schema: false,
                        },
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            // BASE <> BASE TABLE
                            table_types: vec!["BASE".to_string()],
                            include_schema: false,
                        },
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            table_types: vec!["RANDOM".to_string()],
                            include_schema: false,
                        },
                        TestCase {
                            catalog: Some("public"),
                            db_schema_filter_pattern: Some("information_schema"),
                            table_name_filter_pattern: Some("tables"),
                            table_types: vec!["VIEW".to_string()],
                            include_schema: true,
                        },
                    ];

                    let mut client = flightsql_client(state.cluster());

                    let mut output = vec![];
                    for case in cases {
                        let TestCase {
                            catalog,
                            db_schema_filter_pattern,
                            table_name_filter_pattern,
                            table_types,
                            include_schema,
                        } = case;

                        output.push(format!("catalog:{catalog:?}"));
                        output.push(format!(
                            "db_schema_filter_pattern:{db_schema_filter_pattern:?}"
                        ));
                        output.push(format!(
                            "table_name_filter_pattern:{table_name_filter_pattern:?}"
                        ));
                        output.push(format!("table_types:{table_types:?}"));
                        output.push(format!("include_schema:{include_schema:?}"));
                        output.push("*********************".into());

                        let stream = client
                            .get_tables(
                                catalog,
                                db_schema_filter_pattern,
                                table_name_filter_pattern,
                                table_types,
                                include_schema,
                            )
                            .await
                            .unwrap();
                        let batches = collect_stream(stream).await;
                        output.extend(batches_to_sorted_lines(&batches))
                    }

                    insta::assert_yaml_snapshot!(
                        output,
                        @r###"
                    ---
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "table_name_filter_pattern:None"
                    - "table_types:[]"
                    - "include_schema:false"
                    - "*********************"
                    - +--------------+--------------------+-------------+------------+
                    - "| catalog_name | db_schema_name     | table_name  | table_type |"
                    - +--------------+--------------------+-------------+------------+
                    - "| public       | information_schema | columns     | VIEW       |"
                    - "| public       | information_schema | df_settings | VIEW       |"
                    - "| public       | information_schema | tables      | VIEW       |"
                    - "| public       | information_schema | views       | VIEW       |"
                    - "| public       | iox                | the_table   | BASE TABLE |"
                    - "| public       | system             | queries     | BASE TABLE |"
                    - +--------------+--------------------+-------------+------------+
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "table_name_filter_pattern:None"
                    - "table_types:[\"BASE TABLE\"]"
                    - "include_schema:false"
                    - "*********************"
                    - +--------------+----------------+------------+------------+
                    - "| catalog_name | db_schema_name | table_name | table_type |"
                    - +--------------+----------------+------------+------------+
                    - "| public       | iox            | the_table  | BASE TABLE |"
                    - "| public       | system         | queries    | BASE TABLE |"
                    - +--------------+----------------+------------+------------+
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "table_name_filter_pattern:None"
                    - "table_types:[\"BASE\"]"
                    - "include_schema:false"
                    - "*********************"
                    - ++
                    - ++
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "table_name_filter_pattern:None"
                    - "table_types:[\"RANDOM\"]"
                    - "include_schema:false"
                    - "*********************"
                    - ++
                    - ++
                    - "catalog:Some(\"public\")"
                    - "db_schema_filter_pattern:Some(\"information_schema\")"
                    - "table_name_filter_pattern:Some(\"tables\")"
                    - "table_types:[\"VIEW\"]"
                    - "include_schema:true"
                    - "*********************"
                    - +--------------+--------------------+------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                    - "| catalog_name | db_schema_name     | table_name | table_type | table_schema                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |"
                    - +--------------+--------------------+------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                    - "| public       | information_schema | tables     | VIEW       | ffffffff380100001000000000000a000c000a00090004000a00000010000000000104000800080000000400080000000400000004000000a800000064000000340000000400000078ffffff140000000c000000000000050c0000000000000068ffffff0a0000007461626c655f747970650000a4ffffff140000000c000000000000050c0000000000000094ffffff0a0000007461626c655f6e616d650000d0ffffff140000000c000000000000050c00000000000000c0ffffff0c0000007461626c655f736368656d610000000010001400100000000f0004000000080010000000180000000c00000000000005100000000000000004000400040000000d0000007461626c655f636174616c6f670000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 |"
                    - +--------------+--------------------+------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_tables_decoded_table_schema() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from("the_table val=42i 123456")),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let catalog = Some("public");
                    let db_schema_filter_pattern: Option<&str> = None;
                    let table_name_filter_pattern = Some("the_table");
                    let table_types = vec![];
                    let include_schema = true;

                    // verify that the schema that is returned from
                    // GetTables can be properly decoded into an Arrow
                    // schema
                    let stream = client
                        .get_tables(
                            catalog,
                            db_schema_filter_pattern,
                            table_name_filter_pattern,
                            table_types,
                            include_schema,
                        )
                        .await
                        .unwrap();

                    // Code below extracts the <binary> part from the
                    // output below and decodes it as an encoded arrow IPC schema
                    //
                    //| catalog_name | db_schema_name     | table_name | table_type | table_schema
                    //+--------------+--------------------+------------+------------+-------------
                    //|    ...       |      ...           |    ...     |  ...       | <binary>
                    let mut batches = collect_stream(stream).await;
                    assert_eq!(batches.len(), 1);
                    let batch = batches.pop().unwrap();
                    assert_eq!(batch.num_rows(), 1);
                    let array = batch.column_by_name("table_schema").unwrap();
                    let encoded_schema = as_generic_binary_array::<i32>(array).value(0).to_vec();
                    let decoded_schema = decode_schema(Bytes::from(encoded_schema));

                    // Just spot check the schema, the full decoded
                    // content (with metadata, etc) is checked
                    // elsewhere
                    assert_eq!(decoded_schema.fields().len(), 2);
                    let f = decoded_schema.field(0);
                    assert_eq!(f.name(), "time");
                    assert_eq!(
                        f.data_type(),
                        &DataType::Timestamp(TimeUnit::Nanosecond, None)
                    );
                    let f = decoded_schema.field(1);
                    assert_eq!(f.name(), "val");
                    assert_eq!(f.data_type(), &DataType::Int64);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_tables_matches_information_schema() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    // output of get_tables is built manually in IOx, so it is important it remains in sync with the
                    // actual contents of the information schema
                    fn no_filter() -> Option<String> {
                        None
                    }
                    let stream = client
                        .get_tables(no_filter(), no_filter(), no_filter(), vec![], false)
                        .await
                        .unwrap();
                    let get_tables_batches = collect_stream(stream).await;
                    let get_tables_output = batches_to_sorted_lines(&get_tables_batches);

                    let sql = "SELECT table_catalog AS catalog_name, \
                               table_schema AS db_schema_name, table_name, table_type \
                               FROM information_schema.tables \
                               ORDER BY table_catalog, table_schema, table_name, table_type";

                    let stream = client.query(sql).await.unwrap();
                    let information_schema_batches = collect_stream(stream).await;
                    let information_schema_output =
                        batches_to_sorted_lines(&information_schema_batches);

                    insta::assert_yaml_snapshot!(
                        get_tables_output,
                        @r###"
                    ---
                    - +--------------+--------------------+-------------+------------+
                    - "| catalog_name | db_schema_name     | table_name  | table_type |"
                    - +--------------+--------------------+-------------+------------+
                    - "| public       | information_schema | columns     | VIEW       |"
                    - "| public       | information_schema | df_settings | VIEW       |"
                    - "| public       | information_schema | tables      | VIEW       |"
                    - "| public       | information_schema | views       | VIEW       |"
                    - "| public       | iox                | the_table   | BASE TABLE |"
                    - "| public       | system             | queries     | BASE TABLE |"
                    - +--------------+--------------------+-------------+------------+
                    "###
                    );

                    assert_eq!(get_tables_output, information_schema_output);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_table_types() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.get_table_types().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------------+
                    - "| table_type |"
                    - +------------+
                    - "| BASE TABLE |"
                    - "| VIEW       |"
                    - +------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_table_types_matches_information_schema() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    // output of get_table_types is built manually in
                    // IOx, so it is important it remains in sync with
                    // the actual contents of the information schema
                    let stream = client
                        .get_table_types()
                        .await
                        .unwrap();
                    let get_table_types_batches = collect_stream(stream).await;
                    let get_table_types_output = batches_to_sorted_lines(&get_table_types_batches);

                    let sql = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";

                    let stream = client.query(sql).await.unwrap();
                    let information_schema_batches = collect_stream(stream).await;
                    let information_schema_output =
                        batches_to_sorted_lines(&information_schema_batches);

                    insta::assert_yaml_snapshot!(
                        get_table_types_output,
                        @r###"
                    ---
                    - +------------+
                    - "| table_type |"
                    - +------------+
                    - "| BASE TABLE |"
                    - "| VIEW       |"
                    - +------------+
                    "###
                    );

                    assert_eq!(get_table_types_output, information_schema_output);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_db_schemas() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    struct TestCase {
                        catalog: Option<&'static str>,
                        db_schema_filter_pattern: Option<&'static str>,
                    }
                    let cases = [
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                        },
                        TestCase {
                            // pub <> public
                            catalog: Some("pub"),
                            db_schema_filter_pattern: None,
                        },
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: Some("%for%"),
                        },
                        TestCase {
                            catalog: Some("public"),
                            db_schema_filter_pattern: Some("iox"),
                        },
                    ];

                    let mut client = flightsql_client(state.cluster());

                    let mut output = vec![];
                    for case in cases {
                        let TestCase {
                            catalog,
                            db_schema_filter_pattern,
                        } = case;
                        output.push(format!("catalog:{catalog:?}"));
                        output.push(format!(
                            "db_schema_filter_pattern:{db_schema_filter_pattern:?}"
                        ));
                        output.push("*********************".into());

                        let stream = client
                            .get_db_schemas(catalog, db_schema_filter_pattern)
                            .await
                            .unwrap();
                        let batches = collect_stream(stream).await;
                        output.extend(batches_to_sorted_lines(&batches))
                    }
                    insta::assert_yaml_snapshot!(
                        output,
                        @r###"
                    ---
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "*********************"
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - "| public       | iox                |"
                    - "| public       | system             |"
                    - +--------------+--------------------+
                    - "catalog:Some(\"pub\")"
                    - "db_schema_filter_pattern:None"
                    - "*********************"
                    - ++
                    - ++
                    - "catalog:None"
                    - "db_schema_filter_pattern:Some(\"%for%\")"
                    - "*********************"
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - +--------------+--------------------+
                    - "catalog:Some(\"public\")"
                    - "db_schema_filter_pattern:Some(\"iox\")"
                    - "*********************"
                    - +--------------+----------------+
                    - "| catalog_name | db_schema_name |"
                    - +--------------+----------------+
                    - "| public       | iox            |"
                    - +--------------+----------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_db_schema_matches_information_schema() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    // output of get_db_schema is built manually in IOx,
                    // so it is important it remains in sync with the
                    // actual contents of the information schema
                    fn no_filter() -> Option<String> {
                        None
                    }
                    let stream = client
                        .get_db_schemas(no_filter(), no_filter())
                        .await
                        .unwrap();
                    let get_tables_batches = collect_stream(stream).await;
                    let get_tables_output = batches_to_sorted_lines(&get_tables_batches);

                    let sql = "SELECT DISTINCT table_catalog AS catalog_name, table_schema AS db_schema_name \
                               FROM information_schema.tables \
                               ORDER BY table_catalog, table_schema";

                    let stream = client.query(sql).await.unwrap();
                    let information_schema_batches = collect_stream(stream).await;
                    let information_schema_output =
                        batches_to_sorted_lines(&information_schema_batches);

                    insta::assert_yaml_snapshot!(
                        get_tables_output,
                        @r###"
                    ---
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - "| public       | iox                |"
                    - "| public       | system             |"
                    - +--------------+--------------------+
                    "###
                    );

                    assert_eq!(get_tables_output, information_schema_output);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_exported_keys() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let catalog: Option<String> = None;
                    let db_schema: Option<String> = None;

                    let stream = client
                        .get_exported_keys(catalog, db_schema, table_name.to_string())
                        .await
                        .unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - ++
                    - ++
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_imported_keys() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let catalog: Option<String> = None;
                    let db_schema: Option<String> = None;

                    let stream = client
                        .get_imported_keys(catalog, db_schema, table_name.to_string())
                        .await
                        .unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - ++
                    - ++
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_primary_keys() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let catalog: Option<String> = None;
                    let db_schema: Option<String> = None;

                    let stream = client
                        .get_primary_keys(catalog, db_schema, table_name.to_string())
                        .await
                        .unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - ++
                    - ++
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_get_xdbc_type_info() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let data_type: Option<i32> = None;

                    let stream = client.get_xdbc_type_info(data_type).await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    - "| type_name | data_type | column_size | literal_prefix | literal_suffix | create_params | nullable | case_sensitive | searchable | unsigned_attribute | fixed_prec_scale | auto_increment | local_type_name | minimum_scale | maximum_scale | sql_data_type | datetime_subcode | num_prec_radix | interval_precision |"
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    - "| FLOAT     | 6         | 24          |                |                |               | 1        | false          | 3          | false              | false            | false          | FLOAT           |               |               | 6             |                  | 2              |                    |"
                    - "| INTEGER   | 4         | 32          |                |                |               | 1        | false          | 3          | false              | false            | false          | INTEGER         |               |               | 4             |                  | 2              |                    |"
                    - "| INTERVAL  | 10        | 2147483647  | '              | '              |               | 1        | false          | 3          |                    | false            |                | INTERVAL        |               |               | 10            | 0                |                |                    |"
                    - "| TIMESTAMP | 93        | 2147483647  | '              | '              |               | 1        | false          | 3          |                    | false            |                | TIMESTAMP       |               |               | 93            |                  |                |                    |"
                    - "| VARCHAR   | 12        | 2147483647  | '              | '              | [length]      | 1        | true           | 3          |                    | false            |                | VARCHAR         |               |               | 12            |                  |                |                    |"
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    "###
                    );
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    // test filter by type
                    let mut client = flightsql_client(state.cluster());
                    let data_type: Option<i32> = Some(6);

                    let stream = client.get_xdbc_type_info(data_type).await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    - "| type_name | data_type | column_size | literal_prefix | literal_suffix | create_params | nullable | case_sensitive | searchable | unsigned_attribute | fixed_prec_scale | auto_increment | local_type_name | minimum_scale | maximum_scale | sql_data_type | datetime_subcode | num_prec_radix | interval_precision |"
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    - "| FLOAT     | 6         | 24          |                |                |               | 1        | false          | 3          | false              | false            | false          | FLOAT           |               |               | 6             |                  | 2              |                    |"
                    - +-----------+-----------+-------------+----------------+----------------+---------------+----------+----------------+------------+--------------------+------------------+----------------+-----------------+---------------+---------------+---------------+------------------+----------------+--------------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
/// Runs  the `jdbc_client` program against IOx to verify JDBC via FlightSQL is working
///
/// Example command:
///
/// ```shell
/// TEST_INFLUXDB_JDBC=true TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end  jdbc
/// ```
async fn flightsql_jdbc() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    if std::env::var("TEST_INFLUXDB_JDBC").ok().is_none() {
        println!("Skipping JDBC test because TEST_INFLUXDB_JDBC is not set");
        return;
    }

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                // satisfy the borrow checker
                async move {
                    let namespace = state.cluster().namespace();

                    // querier_addr looks like: http://127.0.0.1:8092
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    println!("Querier {querier_addr}, namespace {namespace}");

                    // JDBC URL looks like this:
                    // jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&iox-namespace-name=26f7e5a4b7be365b_917b97a92e883afc
                    let jdbc_addr = querier_addr.replace("http://", "jdbc:arrow-flight-sql://");
                    let jdbc_url =
                        format!("{jdbc_addr}?useEncryption=false&iox-namespace-name={namespace}&iox-debug=true");
                    println!("jdbc_url {jdbc_url}");
                    jdbc_tests(&jdbc_url, table_name).await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
/// Runs  the `jdbc_client` program against IOx to verify authenticated JDBC via FlightSQL is working
///
/// Example command:
///
/// ```shell
/// TEST_INFLUXDB_JDBC=true TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end  jdbc
/// ```
async fn flightsql_jdbc_authz_token() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    if std::env::var("TEST_INFLUXDB_JDBC").ok().is_none() {
        println!("Skipping JDBC test because TEST_INFLUXDB_JDBC is not set");
        return;
    }

    let table_name = "the_table";

    // Set up the authorizer  =================================
    let mut authz = Authorizer::create().await;

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared_with_authz(database_url, authz.addr()).await;

    let write_token = authz.create_token_for(cluster.namespace(), &["ACTION_WRITE"]);
    let read_token =
        authz.create_token_for(cluster.namespace(), &["ACTION_READ", "ACTION_READ_SCHEMA"]);

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocolWithAuthorization {
                line_protocol: format!(
                    "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
                ),
                authorization: format!("Token {}", write_token.clone()),
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = read_token.clone();
                // satisfy the borrow checker
                async move {
                    let namespace = state.cluster().namespace();

                    // querier_addr looks like: http://127.0.0.1:8092
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    println!("Querier {querier_addr}, namespace {namespace}");

                    // JDBC URL looks like this:
                    // jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&iox-namespace-name=26f7e5a4b7be365b_917b97a92e883afc
                    let jdbc_addr = querier_addr.replace("http://", "jdbc:arrow-flight-sql://");
                    let jdbc_url =
                        format!("{jdbc_addr}?useEncryption=false&iox-namespace-name={namespace}&token={token}&iox-debug=true");
                    println!("jdbc_url {jdbc_url}");
                    jdbc_tests(&jdbc_url, table_name).await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await;

    authz.close().await;
}

#[tokio::test]
/// Runs  the `jdbc_client` program against IOx to verify authenticated JDBC via FlightSQL is working
///
/// In this mode the username is empty and password is the authorization token
///
/// Example command:
///
/// ```shell
/// TEST_INFLUXDB_JDBC=true TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end  jdbc
/// ```
async fn flightsql_jdbc_authz_handshake() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    if std::env::var("TEST_INFLUXDB_JDBC").ok().is_none() {
        println!("Skipping JDBC test because TEST_INFLUXDB_JDBC is not set");
        return;
    }

    let table_name = "the_table";

    // Set up the authorizer  =================================
    let mut authz = Authorizer::create().await;

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared_with_authz(database_url, authz.addr()).await;

    let write_token = authz.create_token_for(cluster.namespace(), &["ACTION_WRITE"]);
    let read_token =
        authz.create_token_for(cluster.namespace(), &["ACTION_READ", "ACTION_READ_SCHEMA"]);

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocolWithAuthorization {
                line_protocol: format!(
                    "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
                ),
                authorization: format!("Token {}", write_token.clone()),
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = read_token.clone();
                // satisfy the borrow checker
                async move {
                    let namespace = state.cluster().namespace();

                    // querier_addr looks like: http://127.0.0.1:8092
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    println!("Querier {querier_addr}, namespace {namespace}");

                    // JDBC URL looks like this:
                    // jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&iox-namespace-name=26f7e5a4b7be365b_917b97a92e883afc
                    let jdbc_addr = querier_addr.replace("http://", "jdbc:arrow-flight-sql://");
                    let jdbc_url =
                        format!("{jdbc_addr}?useEncryption=false&iox-namespace-name={namespace}&user=&password={token}&iox-debug=true");
                    println!("jdbc_url {jdbc_url}");
                    jdbc_tests(&jdbc_url, table_name).await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await;

    authz.close().await;
}

async fn jdbc_tests(jdbc_url: &str, table_name: &str) {
    // find the jdbc_client to run
    let path = PathBuf::from(std::env::var("PWD").expect("can not get PWD"))
        .join("influxdb_iox/tests/jdbc_client/jdbc_client");
    println!("Path to jdbc client: {path:?}");

    // Validate basic query: jdbc_client <url> query 'sql'
    Command::new(&path)
        .arg(jdbc_url)
        .arg("query")
        .arg(format!("select * from {table_name} order by time"))
        .output()
        .await
        .unwrap()
        .assert()
        .success()
        .stdout(predicate::str::contains("Running SQL Query"))
        .stdout(predicate::str::contains(
            "A,  B,  1970-01-01 00:00:00.000123456,  42",
        ))
        .stdout(predicate::str::contains(
            "A,  C,  1970-01-01 00:00:00.000123457,  43",
        ));

    // Validate prepared query: jdbc_client <url> prepared_query 'sql'
    Command::new(&path)
        .arg(jdbc_url)
        .arg("prepared_query")
        .arg(format!("select tag1, tag2 from {table_name} order by time"))
        .output()
        .await
        .unwrap()
        .assert()
        .success()
        .stdout(predicate::str::contains("Running Prepared SQL Query"))
        .stdout(predicate::str::contains("A,  B"));

    // CommandGetCatalogs output
    let expected_catalogs = "**************\n\
                             Catalogs:\n\
                             **************\n\
                             TABLE_CAT\n\
                             ------------\n\
                             public";

    // CommandGetSchemas output
    let expected_schemas = "**************\n\
                            Schemas:\n\
                            **************\n\
                            TABLE_SCHEM,  TABLE_CATALOG\n\
                            ------------\n\
                            information_schema,  public\n\
                            iox,  public\n\
                            system,  public";

    // CommandGetTables output
    let expected_tables_no_filter = "**************\n\
                                     Tables:\n\
                                     **************\n\
                                     TABLE_CAT,  TABLE_SCHEM,  TABLE_NAME,  TABLE_TYPE,  REMARKS,  TYPE_CAT,  TYPE_SCHEM,  TYPE_NAME,  SELF_REFERENCING_COL_NAME,  REF_GENERATION\n\
                                     ------------\n\
                                     public,  information_schema,  columns,  VIEW,  null,  null,  null,  null,  null,  null\n\
                                     public,  information_schema,  df_settings,  VIEW,  null,  null,  null,  null,  null,  null\n\
                                     public,  information_schema,  tables,  VIEW,  null,  null,  null,  null,  null,  null\n\
                                     public,  information_schema,  views,  VIEW,  null,  null,  null,  null,  null,  null\n\
                                     public,  iox,  the_table,  BASE TABLE,  null,  null,  null,  null,  null,  null\n\
                                     public,  system,  queries,  BASE TABLE,  null,  null,  null,  null,  null,  null";

    // CommandGetTables output
    let expected_tables_with_filters = "**************\n\
                                        Tables (system table filter):\n\
                                        **************\n\
                                        TABLE_CAT,  TABLE_SCHEM,  TABLE_NAME,  TABLE_TYPE,  REMARKS,  TYPE_CAT,  TYPE_SCHEM,  TYPE_NAME,  SELF_REFERENCING_COL_NAME,  REF_GENERATION\n\
                                        ------------\n\
                                        public,  system,  queries,  BASE TABLE,  null,  null,  null,  null,  null,  null";

    // CommandGetTableTypes output
    let expected_table_types = "**************\n\
                                Table Types:\n\
                                **************\n\
                                TABLE_TYPE\n\
                                ------------\n\
                                BASE TABLE\n\
                                VIEW";

    let expected_xdbc_type_info = "**************\n\
                                        Type Info:\n\
                                        **************\n\
                                        TYPE_NAME,  DATA_TYPE,  PRECISION,  LITERAL_PREFIX,  LITERAL_SUFFIX,  CREATE_PARAMS,  NULLABLE,  CASE_SENSITIVE,  SEARCHABLE,  UNSIGNED_ATTRIBUTE,  FIXED_PREC_SCALE,  AUTO_INCREMENT,  LOCAL_TYPE_NAME,  MINIMUM_SCALE,  MAXIMUM_SCALE,  SQL_DATA_TYPE,  SQL_DATETIME_SUB,  NUM_PREC_RADIX\n\
                                        ------------";

    // Validate metadata: jdbc_client <url> metadata
    let mut assert = Command::new(&path)
        .arg(jdbc_url)
        .arg("metadata")
        .output()
        .await
        .unwrap()
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_catalogs))
        .stdout(predicate::str::contains(expected_schemas))
        .stdout(predicate::str::contains(expected_tables_no_filter))
        .stdout(predicate::str::contains(expected_tables_with_filters))
        .stdout(predicate::str::contains(expected_table_types))
        .stdout(predicate::str::contains(expected_xdbc_type_info));

    let expected_metadata = EXPECTED_METADATA
        .trim()
        .replace("REPLACE_ME_WITH_JBDC_URL", jdbc_url);

    for expected in expected_metadata.lines() {
        assert = assert.stdout(predicate::str::contains(expected));
    }
}

/// Ensures that the schema returned as part of GetFlightInfo matches
/// that of the actual response (as the schema is hard coded in some
/// cases)
#[tokio::test]
async fn flightsql_schema_matches() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster()).into_inner();

                    // Verify schema for each type of command
                    let cases = vec![
                        CommandStatementQuery {
                            query: format!("select * from {table_name}"),
                            transaction_id: None,
                        }
                        .as_any(),
                        CommandGetSqlInfo { info: vec![] }.as_any(),
                        CommandGetCatalogs {}.as_any(),
                        CommandGetDbSchemas {
                            catalog: None,
                            db_schema_filter_pattern: None,
                        }
                        .as_any(),
                        CommandGetTables {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            table_types: vec![],
                            // Do not include optional `table_schema` column
                            include_schema: false,
                        }
                        .as_any(),
                        CommandGetTables {
                            catalog: None,
                            db_schema_filter_pattern: None,
                            table_name_filter_pattern: None,
                            table_types: vec![],
                            // Include optional `table_schema` column
                            include_schema: true,
                        }
                        .as_any(),
                        CommandGetTableTypes {}.as_any(),
                    ];

                    for cmd in cases {
                        assert_schema(&mut client, cmd).await;
                    }
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

///  Verifies that the schema returned by `GetFlightInfo` and `DoGet`
///  match for `cmd`.
async fn assert_schema(client: &mut FlightClient, cmd: Any) {
    println!("Checking schema for message type {}", cmd.type_url);

    let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
    let flight_info = client.get_flight_info(descriptor).await.unwrap();

    assert_eq!(flight_info.endpoint.len(), 1);
    let ticket = flight_info.endpoint[0]
        .ticket
        .as_ref()
        .expect("Need ticket")
        .clone();

    // Schema reported by `GetFlightInfo`
    let flight_info_schema = flight_info.try_decode_schema().unwrap();

    // Get results and ensure they match the schema reported by GetFlightInfo
    let mut result_stream = client.do_get(ticket).await.unwrap();
    let mut saw_data = false;
    while let Some(batch) = result_stream.try_next().await.unwrap() {
        saw_data = true;
        let batch_schema = batch.schema();
        assert_eq!(
            batch_schema.as_ref(),
            &flight_info_schema,
            "batch_schema:\n{batch_schema:#?}\n\nflight_info_schema:\n{flight_info_schema:#?}"
        );
        // The stream itself also may report a schema
        if let Some(stream_schema) = result_stream.schema() {
            assert_eq!(stream_schema.as_ref(), &flight_info_schema);
        }
    }
    // verify we have seen at least one RecordBatch
    // (all FlightSQL endpoints return at least one)
    assert!(saw_data);
}

#[tokio::test]
async fn authz() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the authorizer  =================================
    let mut authz = Authorizer::create().await;

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared_with_authz(database_url, authz.addr()).await;

    let write_token = authz.create_token_for(cluster.namespace(), &["ACTION_WRITE"]);
    let read_token = authz.create_token_for(cluster.namespace(), &["ACTION_READ_SCHEMA"]);

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocolWithAuthorization {
                line_protocol: format!(
                    "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
                ),
                authorization: format!("Token {}", write_token.clone()),
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());
                    let err = client.get_table_types().await.unwrap_err();

                    assert_matches!(err, FlightError::Tonic(status) => {
                                assert_eq!(tonic::Code::Unauthenticated, status.code());
                                assert_eq!("Unauthenticated", status.message());
                    });
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = write_token.clone();
                async move {
                    let mut client = flightsql_client(state.cluster());
                    client
                        .add_header(
                            "authorization",
                            format!("Bearer {}", token.clone()).as_str(),
                        )
                        .unwrap();
                    let err = client.get_table_types().await.unwrap_err();

                    assert_matches!(err, FlightError::Tonic(status) => {
                                assert_eq!(tonic::Code::PermissionDenied, status.code());
                                assert_eq!("Permission denied", status.message());
                    });
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = read_token.clone();
                async move {
                    let mut client = flightsql_client(state.cluster());
                    client
                        .add_header(
                            "authorization",
                            format!("Bearer {}", token.clone()).as_str(),
                        )
                        .unwrap();

                    let stream = client.get_table_types().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------------+
                    - "| table_type |"
                    - +------------+
                    - "| BASE TABLE |"
                    - "| VIEW       |"
                    - +------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await;

    authz.close().await;
}

/// Ensure that FligthSQL API supports the following grpc header names,
/// in addition to the existing `iox-namespace-name`
///   1. database
///   2. bucket
///   3. bucket-name
#[tokio::test]
async fn flightsql_client_header_same_database() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client_helper(state.cluster(), "iox-namespace-name");
                    for header_name in &["database", "bucket", "bucket-name"] {
                        // different header names with the same database name
                        client
                            .add_header(header_name, state.cluster().namespace())
                            .unwrap();
                    }

                    let stream = client.get_table_types().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------------+
                    - "| table_type |"
                    - +------------+
                    - "| BASE TABLE |"
                    - "| VIEW       |"
                    - +------------+
                    "###
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_client_header_different_database() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client_helper(state.cluster(), "database");
                    client
                        .add_header("bucket", "different_database_name")
                        .unwrap();

                    let err = client.get_table_types().await.unwrap_err();

                    assert_matches!(err, FlightError::Tonic(status) => {
                        assert_eq!(status.code(), tonic::Code::InvalidArgument);
                        assert_contains!(status.message(), "More than one headers are found in request");
                    }
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn flightsql_client_header_no_database() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let connection = state.cluster().querier().querier_grpc_connection();
                    let (channel, _headers) = connection.into_grpc_connection().into_parts();

                    let mut client = FlightSqlClient::new(channel);

                    let err = client.get_table_types().await.unwrap_err();

                    assert_matches!(err, FlightError::Tonic(status) => {
                        assert_eq!(status.code(), tonic::Code::InvalidArgument);
                        assert_contains!(status.message(), "no 'database' header in request");
                    }
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Return a [`FlightSqlClient`] configured for use
fn flightsql_client(cluster: &MiniCluster) -> FlightSqlClient {
    flightsql_client_helper(cluster, "database")
}

/// Helper function for fn `flightsql_client` that returns a [`FlightSqlClient`] configured for use
fn flightsql_client_helper(cluster: &MiniCluster, header_name: &str) -> FlightSqlClient {
    let connection = cluster.querier().querier_grpc_connection();
    let (channel, _headers) = connection.into_grpc_connection().into_parts();

    let mut client = FlightSqlClient::new(channel);

    // Add namespace to client headers until it is fully supported by FlightSQL
    let namespace = cluster.namespace();
    client.add_header(header_name, namespace).unwrap();
    client.add_header("iox-debug", "true").unwrap();

    client
}

async fn collect_stream(stream: FlightRecordBatchStream) -> Vec<RecordBatch> {
    stream.try_collect().await.expect("collecting batches")
}

fn decode_schema(data: Bytes) -> Schema {
    let msg = IpcMessage(data);
    Schema::try_from(msg).unwrap()
}

const EXPECTED_METADATA: &str = r#"
allProceduresAreCallable: true
allTablesAreSelectable: true
autoCommitFailureClosesAllResultSets: false
dataDefinitionCausesTransactionCommit: false
dataDefinitionIgnoredInTransactions: true
doesMaxRowSizeIncludeBlobs: true
generatedKeyAlwaysReturned: false
getCatalogSeparator: .
getCatalogTerm: null
getDatabaseMajorVersion: 10
getDatabaseMinorVersion: 0
getDatabaseProductName: InfluxDB IOx
getDatabaseProductVersion: 2
getDefaultTransactionIsolation: 0
getDriverMajorVersion: 10
getDriverMinorVersion: 0
getDriverName: Arrow Flight SQL JDBC Driver
getDriverVersion: 10.0.0
getExtraNameCharacters:
getIdentifierQuoteString: "
getJDBCMajorVersion: 4
getJDBCMinorVersion: 1
getMaxBinaryLiteralLength: 2147483647
getMaxCatalogNameLength: 2147483647
getMaxCharLiteralLength: 2147483647
getMaxColumnNameLength: 2147483647
getMaxColumnsInGroupBy: 2147483647
getMaxColumnsInIndex: 2147483647
getMaxColumnsInOrderBy: 2147483647
getMaxColumnsInSelect: 2147483647
getMaxColumnsInTable: 2147483647
getMaxConnections: 2147483647
getMaxCursorNameLength: 2147483647
getMaxIndexLength: 2147483647
getMaxLogicalLobSize: 0
getMaxProcedureNameLength: 2147483647
getMaxRowSize: 2147483647
getMaxSchemaNameLength: 2147483647
getMaxStatementLength: 2147483647
getMaxStatements: 2147483647
getMaxTableNameLength: 2147483647
getMaxTablesInSelect: 2147483647
getMaxUserNameLength: 2147483647
getNumericFunctions: abs, acos, asin, atan, atan2, ceil, cos, exp, floor, ln, log, log10, log2, pow, power, round, signum, sin, sqrt, tan, trunc
getProcedureTerm: procedure
getResultSetHoldability: 1
getSchemaTerm: schema
getSearchStringEscape: \
getSQLKeywords: absolute, action, add, all, allocate, alter, and, any, are, as, asc, assertion, at, authorization, avg, begin, between, bit, bit_length, both, by, cascade, cascaded, case, cast, catalog, char, char_length, character, character_length, check, close, coalesce, collate, collation, column, commit, connect, connection, constraint, constraints, continue, convert, corresponding, count, create, cross, current, current_date, current_time, current_timestamp, current_user, cursor, date, day, deallocate, dec, decimal, declare, default, deferrable, deferred, delete, desc, describe, descriptor, diagnostics, disconnect, distinct, domain, double, drop, else, end, end-exec, escape, except, exception, exec, execute, exists, external, extract, false, fetch, first, float, for, foreign, found, from, full, get, global, go, goto, grant, group, having, hour, identity, immediate, in, indicator, initially, inner, input, insensitive, insert, int, integer, intersect, interval, into, is, isolation, join, key, language, last, leading, left, level, like, local, lower, match, max, min, minute, module, month, names, national, natural, nchar, next, no, not, null, nullif, numeric, octet_length, of, on, only, open, option, or, order, outer, output, overlaps, pad, partial, position, precision, prepare, preserve, primary, prior, privileges, procedure, public, read, real, references, relative, restrict, revoke, right, rollback, rows, schema, scroll, second, section, select, session, session_user, set, size, smallint, some, space, sql, sqlcode, sqlerror, sqlstate, substring, sum, system_user, table, temporary, then, time, timestamp, timezone_hour, timezone_minute, to, trailing, transaction, translate, translation, trim, true, union, unique, unknown, update, upper, usage, user, using, value, values, varchar, varying, view, when, whenever, where, with, work, write, year, zone
getSQLStateType: 2
getStringFunctions: arrow_typeof, ascii, bit_length, btrim, char_length, character_length, chr, concat, concat_ws, digest, from_unixtime, initcap, left, length, lower, lpad, ltrim, md5, octet_length, random, regexp_match, regexp_replace, repeat, replace, reverse, right, rpad, rtrim, sha224, sha256, sha384, sha512, split_part, starts_with, strpos, substr, to_hex, translate, trim, upper, uuid
getSystemFunctions: array, arrow_typeof, struct
getTimeDateFunctions: current_date, current_time, date_bin, date_part, date_trunc, datepart, datetrunc, from_unixtime, now, to_timestamp, to_timestamp_micros, to_timestamp_millis, to_timestamp_seconds
getURL: REPLACE_ME_WITH_JBDC_URL
isCatalogAtStart: false
isReadOnly: true
locatorsUpdateCopy: false
nullPlusNonNullIsNull: true
nullsAreSortedAtEnd: true
nullsAreSortedAtStart: false
nullsAreSortedHigh: false
nullsAreSortedLow: false
storesLowerCaseIdentifiers: false
storesLowerCaseQuotedIdentifiers: false
storesMixedCaseIdentifiers: false
storesMixedCaseQuotedIdentifiers: false
storesUpperCaseIdentifiers: true
storesUpperCaseQuotedIdentifiers: false
supportsAlterTableWithAddColumn: false
supportsAlterTableWithDropColumn: false
supportsANSI92EntryLevelSQL: true
supportsANSI92FullSQL: true
supportsANSI92IntermediateSQL: true
supportsBatchUpdates: false
supportsCatalogsInDataManipulation: true
supportsCatalogsInIndexDefinitions: false
supportsCatalogsInPrivilegeDefinitions: false
supportsCatalogsInProcedureCalls: true
supportsCatalogsInTableDefinitions: true
supportsColumnAliasing: true
supportsCoreSQLGrammar: false
supportsCorrelatedSubqueries: true
supportsDataDefinitionAndDataManipulationTransactions: false
supportsDataManipulationTransactionsOnly: true
supportsDifferentTableCorrelationNames: false
supportsExpressionsInOrderBy: true
supportsExtendedSQLGrammar: false
supportsFullOuterJoins: false
supportsGetGeneratedKeys: false
supportsGroupBy: true
supportsGroupByBeyondSelect: true
supportsGroupByUnrelated: true
supportsIntegrityEnhancementFacility: false
supportsLikeEscapeClause: true
supportsLimitedOuterJoins: true
supportsMinimumSQLGrammar: true
supportsMixedCaseIdentifiers: false
supportsMixedCaseQuotedIdentifiers: true
supportsMultipleOpenResults: false
supportsMultipleResultSets: false
supportsMultipleTransactions: false
supportsNamedParameters: false
supportsNonNullableColumns: true
supportsOpenCursorsAcrossCommit: false
supportsOpenCursorsAcrossRollback: false
supportsOpenStatementsAcrossCommit: false
supportsOpenStatementsAcrossRollback: false
supportsOrderByUnrelated: true
supportsOuterJoins: true
supportsPositionedDelete: false
supportsPositionedUpdate: false
supportsRefCursors: false
supportsSavepoints: false
supportsSchemasInDataManipulation: true
supportsSchemasInIndexDefinitions: false
supportsSchemasInPrivilegeDefinitions: false
supportsSchemasInProcedureCalls: false
supportsSchemasInTableDefinitions: true
supportsSelectForUpdate: false
supportsStatementPooling: false
supportsStoredFunctionsUsingCallSyntax: false
supportsStoredProcedures: false
supportsSubqueriesInComparisons: true
supportsSubqueriesInExists: true
supportsSubqueriesInIns: true
supportsSubqueriesInQuantifieds: true
supportsTableCorrelationNames: false
supportsTransactionIsolationLevel: false
supportsTransactions: false
supportsUnion: true
supportsUnionAll: true
usesLocalFilePerTable: false
usesLocalFiles: false
"#;
