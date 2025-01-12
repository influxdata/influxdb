use crate::server::{enterprise::tmp_dir, ConfigProvider, TestServer};
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use serde_json::json;

#[tokio::test]
async fn file_index_config() {
    let obj_store_path = tmp_dir();
    let idx_server = TestServer::configure_pro()
        .with_writer_id("idx_server")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;
    idx_server
        .write_lp_to_db(
            "gundam",
            "unicorn,height=19.7,id=\"RX-0\" health=0.5 94\n\
             mercury,height=18.0,id=\"XVX-016\" health=1.0 104",
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

    idx_server
        .api_v3_configure_file_index_create(&json!({
            "db": "gundam",
            "table": "unicorn",
            "columns": ["id", "health"]
        }))
        .await;

    assert_eq!(
        "\
         +---------------+------------+---------------+------------------+\n\
         | database_name | table_name | index_columns | index_column_ids |\n\
         +---------------+------------+---------------+------------------+\n\
         | gundam        | unicorn    | [id, health]  | [1, 2]           |\n\
         +---------------+------------+---------------+------------------+",
        query().await
    );

    // add just a db column to the index
    idx_server
        .api_v3_configure_file_index_create(&json!({
            "db": "gundam",
            "table": null,
            "columns": ["height"]
        }))
        .await;
    assert_eq!(
        "\
         +---------------+------------+----------------------+------------------+\n\
         | database_name | table_name | index_columns        | index_column_ids |\n\
         +---------------+------------+----------------------+------------------+\n\
         | gundam        |            | [height]             | []               |\n\
         | gundam        | unicorn    | [height, id, health] | [0, 1, 2]        |\n\
         +---------------+------------+----------------------+------------------+",
        query().await
    );

    // add another table column to the index
    idx_server
        .api_v3_configure_file_index_create(&json!({
            "db": "gundam",
            "table": "mercury",
            "columns": ["time"]
        }))
        .await;
    assert_eq!(
        "\
         +---------------+------------+----------------------+------------------+\n\
         | database_name | table_name | index_columns        | index_column_ids |\n\
         +---------------+------------+----------------------+------------------+\n\
         | gundam        |            | [height]             | []               |\n\
         | gundam        | unicorn    | [height, id, health] | [0, 1, 2]        |\n\
         | gundam        | mercury    | [height, time]       | [4, 7]           |\n\
         +---------------+------------+----------------------+------------------+",
        query().await
    );
    // Test that dropping a table from the index only drops that table
    idx_server
        .api_v3_configure_file_index_delete(&json!({
            "db": "gundam",
            "table": "unicorn",
        }))
        .await;
    assert_eq!(
        "\
         +---------------+------------+----------------+------------------+\n\
         | database_name | table_name | index_columns  | index_column_ids |\n\
         +---------------+------------+----------------+------------------+\n\
         | gundam        |            | [height]       | []               |\n\
         | gundam        | mercury    | [height, time] | [4, 7]           |\n\
         +---------------+------------+----------------+------------------+",
        query().await
    );
    // Test that dropping the db from the index drops all tables under it
    idx_server
        .api_v3_configure_file_index_delete(&json!({
            "db": "gundam",
            "table": null,
        }))
        .await;
    assert_eq!(
        "\
         ++\n\
         ++",
        query().await
    );
}
