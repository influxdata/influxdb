use crate::server::TestServer;
use assert_cmd::cargo::CommandCargoExt;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use std::process::Command;
use std::process::Stdio;

pub struct CLI;

impl CLI {
    pub fn run(args: &[&str]) -> String {
        String::from_utf8(
            Command::cargo_bin("influxdb3")
                .unwrap()
                .args(args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap()
                .wait_with_output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .into()
    }
}

#[tokio::test]
async fn configure_file_index() {
    let server = TestServer::spawn().await;
    server
        .write_lp_to_db(
            "gundam",
            "unicorn,height=19.7,id=\"RX-0\" health=0.5 94\n\
             mercury,height=18.0,id=\"XVX-016\" health=1.0 104",
            Precision::Millisecond,
        )
        .await
        .unwrap();
    let addr = server.client_addr();
    let query = || {
        CLI::run(&[
            "query",
            "-h",
            addr.as_str(),
            "-d",
            "gundam",
            "select * from system.file_index",
        ])
    };
    CLI::run(&[
        "configure",
        "create",
        "file-index",
        "-h",
        addr.as_str(),
        "-d",
        "gundam",
        "--table",
        "unicorn",
        "id",
        "health",
    ]);

    assert_eq!(
        "\
         +---------------+------------+---------------+\n\
         | database_name | table_name | index_columns |\n\
         +---------------+------------+---------------+\n\
         | gundam        | unicorn    | id,health     |\n\
         +---------------+------------+---------------+",
        query()
    );
    CLI::run(&[
        "configure",
        "create",
        "file-index",
        "-h",
        addr.as_str(),
        "-d",
        "gundam",
        "height",
    ]);
    assert_eq!(
        "\
         +---------------+------------+------------------+\n\
         | database_name | table_name | index_columns    |\n\
         +---------------+------------+------------------+\n\
         | gundam        |            | height           |\n\
         | gundam        | unicorn    | height,id,health |\n\
         +---------------+------------+------------------+",
        query()
    );

    CLI::run(&[
        "configure",
        "create",
        "file-index",
        "-h",
        addr.as_str(),
        "-d",
        "gundam",
        "--table",
        "mercury",
        "time",
    ]);
    assert_eq!(
        "\
         +---------------+------------+------------------+\n\
         | database_name | table_name | index_columns    |\n\
         +---------------+------------+------------------+\n\
         | gundam        |            | height           |\n\
         | gundam        | unicorn    | height,id,health |\n\
         | gundam        | mercury    | height,time      |\n\
         +---------------+------------+------------------+",
        query()
    );
    CLI::run(&[
        "configure",
        "delete",
        "file-index",
        "-h",
        addr.as_str(),
        "-d",
        "gundam",
        "--table",
        "unicorn",
    ]);
    assert_eq!(
        "\
         +---------------+------------+---------------+\n\
         | database_name | table_name | index_columns |\n\
         +---------------+------------+---------------+\n\
         | gundam        |            | height        |\n\
         | gundam        | mercury    | height,time   |\n\
         +---------------+------------+---------------+",
        query()
    );
    CLI::run(&[
        "configure",
        "delete",
        "file-index",
        "-h",
        addr.as_str(),
        "-d",
        "gundam",
    ]);
    assert_eq!(
        "\
         ++\n\
         ++",
        query()
    );
}
