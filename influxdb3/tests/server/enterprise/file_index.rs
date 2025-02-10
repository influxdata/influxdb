use crate::server::{ConfigProvider, TestServer, enterprise::tmp_dir};
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test_log::test(tokio::test)]
async fn file_index_config() {
    let obj_store_path = tmp_dir();
    let idx_server = TestServer::configure_enterprise()
        .with_node_id("idx_server")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;
    idx_server
        .write_lp_to_db(
            "gundam",
            "unicorn,id=RX-0,operator=londo height=19.7,health=0.5,status=\"active\" 94\n\
             mercury,id=XVX-016,operator=earth height=18.0,health=1.0,status=\"active\" 104",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    let query = || async {
        idx_server
            .api_v3_query_sql(&[
                ("db", "gundam"),
                ("q", "SELECT * FROM system.file_index"),
                ("format", "pretty"),
            ])
            .await
            .text()
            .await
            .expect("do query")
    };

    // Test that adding an index works:
    assert!(
        idx_server
            .api_v3_configure_file_index_create(&json!({
                "db": "gundam",
                "table": "unicorn",
                "columns": ["id"]
            }))
            .await
            .status()
            .is_success()
    );

    assert_eq!(
        "\
         +---------------+------------+---------------+------------------+\n\
         | database_name | table_name | index_columns | index_column_ids |\n\
         +---------------+------------+---------------+------------------+\n\
         | gundam        | unicorn    | [id]          | [0]              |\n\
         +---------------+------------+---------------+------------------+",
        query().await
    );

    // add just a db column to the index
    assert!(
        idx_server
            .api_v3_configure_file_index_create(&json!({
                "db": "gundam",
                "columns": ["height"]
            }))
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        "\
         +---------------+------------+---------------+------------------+\n\
         | database_name | table_name | index_columns | index_column_ids |\n\
         +---------------+------------+---------------+------------------+\n\
         | gundam        |            | [height]      | []               |\n\
         | gundam        | unicorn    | [id]          | [0]              |\n\
         +---------------+------------+---------------+------------------+",
        query().await
    );

    // add another table to the index
    assert!(
        idx_server
            .api_v3_configure_file_index_create(&json!({
                "db": "gundam",
                "table": "mercury",
                "columns": ["operator"]
            }))
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        "\
         +---------------+------------+---------------+------------------+\n\
         | database_name | table_name | index_columns | index_column_ids |\n\
         +---------------+------------+---------------+------------------+\n\
         | gundam        |            | [height]      | []               |\n\
         | gundam        | unicorn    | [id]          | [0]              |\n\
         | gundam        | mercury    | [operator]    | [1]              |\n\
         +---------------+------------+---------------+------------------+",
        query().await
    );

    // add another column to existing table to the index
    assert!(
        idx_server
            .api_v3_configure_file_index_create(&json!({
                "db": "gundam",
                "table": "unicorn",
                "columns": ["operator"]
            }))
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        "\
         +---------------+------------+----------------+------------------+\n\
         | database_name | table_name | index_columns  | index_column_ids |\n\
         +---------------+------------+----------------+------------------+\n\
         | gundam        |            | [height]       | []               |\n\
         | gundam        | unicorn    | [id, operator] | [0, 1]           |\n\
         | gundam        | mercury    | [operator]     | [1]              |\n\
         +---------------+------------+----------------+------------------+",
        query().await
    );

    // Test that dropping a table from the index only drops that table
    assert!(
        idx_server
            .api_v3_configure_file_index_delete(&json!({
                "db": "gundam",
                "table": "unicorn",
            }))
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        "\
         +---------------+------------+---------------+------------------+\n\
         | database_name | table_name | index_columns | index_column_ids |\n\
         +---------------+------------+---------------+------------------+\n\
         | gundam        |            | [height]      | []               |\n\
         | gundam        | mercury    | [operator]    | [1]              |\n\
         +---------------+------------+---------------+------------------+",
        query().await
    );

    // Test that dropping the db from the index drops all tables under it
    assert!(
        idx_server
            .api_v3_configure_file_index_delete(&json!({
                "db": "gundam",
                "table": null,
            }))
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        "\
         ++\n\
         ++",
        query().await
    );

    // Test that adding an index with a non-tag or string column fails:
    assert!(
        idx_server
            .api_v3_configure_file_index_create(&json!({
                "db": "gundam",
                "table": "unicorn",
                "columns": ["health"]
            }))
            .await
            .status()
            .is_client_error()
    );
}
