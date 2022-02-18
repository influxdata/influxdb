use crate::common::server_fixture::TestConfig;
use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::rand_name,
};
use assert_cmd::Command;
use generated_types::influxdata::iox::router::v1::{write_sink::Sink, Router};
use predicates::prelude::*;
use std::io::Write;

#[tokio::test]
async fn test_router_crud() {
    let server_fixture = ServerFixture::create_shared(ServerType::Router).await;
    let addr = server_fixture.grpc_base();
    let router_name = rand_name();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("get")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(format!(
            "Resource router/{} not found",
            router_name,
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("create-or-update")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Created/Updated router {}",
            router_name
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(&router_name));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("get")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(&router_name).and(predicate::str::contains(format!(
                r#""name": "{}"#,
                &router_name
            ))), // validate the defaults have been set reasonably
        );

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("delete")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Deleted router {}",
            router_name
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(&router_name).not());
}

#[tokio::test]
async fn test_router_static_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();

    let config = r#"
    {
      "routers":[
        {
          "name":"foo",
          "writeSharder":{
            "hashRing":{
              "shards":[
                1
              ]
            }
          },
          "writeSinks":{
            "1":{
              "sinks":[
                {
                  "grpcRemote":12
                }
              ]
            }
          }
        }
      ],
      "templates":[
        {
          "name":"default",
          "writeSharder":{
            "specificTargets":[
              {
                "matcher":{
                  "tableNameRegex":".*"
                },
                "shard":1
              }
            ]
          },
          "writeSinks":{
            "1":{
              "sinks":[
                {
                  "writeBuffer":{
                    "type":"kafka",
                    "connection":"kafka-bootstrap:9092",
                    "creationConfig":{
                      "nSequencers":1
                    }
                  }
                }
              ]
            }
          }
        }
      ],
      "instances": [
        {
          "name": "instance1",
          "template": "default"
        },
        {
          "name": "instance2",
          "template": "default"
        }
      ]
    }"#;

    write!(file, "{}", config).unwrap();

    let config = TestConfig::new(ServerType::Router)
        .with_env("INFLUXDB_IOX_CONFIG_FILE", file.path().to_str().unwrap());

    let server_fixture = ServerFixture::create_single_use_with_config(config).await;
    let addr = server_fixture.grpc_base();
    let router_name = rand_name();

    // Can list routers
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("foo"));

    // Can get router configuration
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("get")
        .arg("foo")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("\"grpcRemote\": 12"));

    // Template instantiated
    for instance in ["instance1", "instance2"] {
        let output = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("router")
            .arg("get")
            .arg(instance)
            .arg("--host")
            .arg(addr)
            .assert()
            .success();

        let decoded: Router = serde_json::from_slice(&output.get_output().stdout).unwrap();
        assert_eq!(decoded.name, instance);
        match &decoded.write_sinks.get(&1).unwrap().sinks[0].sink {
            Some(Sink::WriteBuffer(x)) => {
                assert_eq!(x.creation_config.as_ref().unwrap().n_sequencers, 1);
                assert_eq!(x.connection, "kafka-bootstrap:9092");
            }
            _ => panic!(
                "unexpected output: {}",
                String::from_utf8_lossy(&output.get_output().stdout)
            ),
        }
    }

    // Cannot create router
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("create-or-update")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "router configuration is not mutable",
        ));

    // Cannot delete router
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("delete")
        .arg("foo")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "router configuration is not mutable",
        ));

    // Cleanup temporary file
    std::mem::drop(file);
}
