//! Client helpers for writing end to end ng tests
use arrow::record_batch::RecordBatch;
use futures::{stream::FuturesUnordered, StreamExt};
use http::Response;
use hyper::{Body, Client, Request};
use influxdb_iox_client::{
    connection::Connection,
    flight::generated_types::ReadInfo,
    write::generated_types::{DatabaseBatch, TableBatch, WriteRequest, WriteResponse},
    write_info::generated_types::{GetWriteInfoResponse, KafkaPartitionInfo, KafkaPartitionStatus},
};
use observability_deps::tracing::info;
use std::{collections::HashMap, time::Duration};

/// Writes the line protocol to the write_base/api/v2/write endpoint (typically on the router)
pub async fn write_to_router(
    line_protocol: impl Into<String>,
    org: impl AsRef<str>,
    bucket: impl AsRef<str>,
    write_base: impl AsRef<str>,
) -> Response<Body> {
    let client = Client::new();
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}",
        write_base.as_ref(),
        org.as_ref(),
        bucket.as_ref()
    );

    let request = Request::builder()
        .uri(url)
        .method("POST")
        .body(Body::from(line_protocol.into()))
        .expect("failed to construct HTTP request");

    client
        .request(request)
        .await
        .expect("http error sending write")
}

/// Writes the table batch to the gRPC write API on the router into the org/bucket (typically on
/// the router)
pub async fn write_to_router_grpc(
    table_batches: Vec<TableBatch>,
    namespace: impl Into<String>,
    router2_connection: Connection,
) -> tonic::Response<WriteResponse> {
    let request = WriteRequest {
        database_batch: Some(DatabaseBatch {
            database_name: namespace.into(),
            table_batches,
        }),
    };

    influxdb_iox_client::write::Client::new(router2_connection)
        .write_pb(request)
        .await
        .expect("grpc error sending write")
}

/// Extracts the write token from the specified response (to the /api/v2/write api)
pub fn get_write_token(response: &Response<Body>) -> String {
    let message = format!("no write token in {:?}", response);
    response
        .headers()
        .get("X-IOx-Write-Token")
        .expect(&message)
        .to_str()
        .expect("Value not a string")
        .to_string()
}

/// Extracts the write token from the specified response (to the gRPC write API)
pub fn get_write_token_from_grpc(response: &tonic::Response<WriteResponse>) -> String {
    let message = format!("no write token in {:?}", response);
    response
        .metadata()
        .get("X-IOx-Write-Token")
        .expect(&message)
        .to_str()
        .expect("Value not a string")
        .to_string()
}

/// returns the write info from ingester_connection for this token
pub async fn token_info(
    write_token: impl AsRef<str>,
    ingester_connection: Connection,
) -> Result<GetWriteInfoResponse, influxdb_iox_client::error::Error> {
    influxdb_iox_client::write_info::Client::new(ingester_connection)
        .get_write_info(write_token.as_ref())
        .await
}

/// returns a combined write info that contains the combined
/// information across all ingester_connections for all the specified
/// tokens
pub async fn combined_token_info(
    write_tokens: Vec<String>,
    ingester_connections: Vec<Connection>,
) -> Result<GetWriteInfoResponse, influxdb_iox_client::error::Error> {
    let responses = write_tokens
        .into_iter()
        .flat_map(|write_token| {
            ingester_connections
                .clone()
                .into_iter()
                .map(move |ingester_connection| {
                    token_info(write_token.clone(), ingester_connection)
                })
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await
        // check for errors
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    info!("combining response: {:#?}", responses);

    // merge them together
    Ok(merge_responses(responses))
}

/// returns true if the write for this token is persisted, false if it
/// is not persisted. panic's on error
pub async fn token_is_persisted(
    write_token: impl AsRef<str>,
    ingester_connection: Connection,
) -> bool {
    let res = token_info(write_token, ingester_connection)
        .await
        .expect("Error fetching write info for token");
    all_persisted(&res)
}

const MAX_QUERY_RETRY_TIME_SEC: u64 = 20;

/// Waits for the specified predicate to return true
pub async fn wait_for_token<F>(
    write_token: impl Into<String>,
    ingester_connection: Connection,
    f: F,
) where
    F: Fn(&GetWriteInfoResponse) -> bool,
{
    let write_token = write_token.into();
    assert!(!write_token.is_empty());

    info!("  write token: {}", write_token);

    let retry_duration = Duration::from_secs(MAX_QUERY_RETRY_TIME_SEC);
    let mut write_info_client = influxdb_iox_client::write_info::Client::new(ingester_connection);
    tokio::time::timeout(retry_duration, async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            match write_info_client.get_write_info(&write_token).await {
                Ok(res) => {
                    if f(&res) {
                        return;
                    }
                    info!("Retrying; predicate not satistified: {:?}", res);
                }

                Err(e) => {
                    info!("Retrying; Got error getting write_info: {}", e);
                }
            };
            interval.tick().await;
        }
    })
    .await
    .expect("did not get passing predicate on token");
}

/// Waits for the specified write token to be readable
pub async fn wait_for_readable(write_token: impl Into<String>, ingester_connection: Connection) {
    info!("Waiting for write token to be readable");

    wait_for_token(write_token, ingester_connection, |res| {
        if all_readable(res) {
            info!("Write is readable: {:?}", res);
            true
        } else {
            false
        }
    })
    .await
}

/// Waits for the write token to be persisted
pub async fn wait_for_persisted(write_token: impl Into<String>, ingester_connection: Connection) {
    info!("Waiting for write token to be persisted");

    wait_for_token(write_token, ingester_connection, |res| {
        if all_persisted(res) {
            info!("Write is persisted: {:?}", res);
            true
        } else {
            false
        }
    })
    .await
}

/// returns true if all partitions in the response are readablel
/// TODO: maybe put this in the influxdb_iox_client library / make a
/// proper public facing client API. For now, iterate in the end to end tests.
pub fn all_readable(res: &GetWriteInfoResponse) -> bool {
    res.kafka_partition_infos.iter().all(|info| {
        matches!(
            info.status(),
            KafkaPartitionStatus::Readable | KafkaPartitionStatus::Persisted
        )
    })
}

/// returns true if all partitions in the response are readablel
/// TODO: maybe put this in the influxdb_iox_client library / make a
/// proper public facing client API. For now, iterate in the end to end tests.
pub fn all_persisted(res: &GetWriteInfoResponse) -> bool {
    res.kafka_partition_infos
        .iter()
        .all(|info| matches!(info.status(), KafkaPartitionStatus::Persisted))
}

/// "merges" the partition information for write info responses so
/// that the "most recent" information is returned
fn merge_responses(
    responses: impl IntoIterator<Item = GetWriteInfoResponse>,
) -> GetWriteInfoResponse {
    // make kafka partition id to status
    let mut partition_infos: HashMap<_, KafkaPartitionInfo> = HashMap::new();

    responses
        .into_iter()
        .flat_map(|res| res.kafka_partition_infos.into_iter())
        .for_each(|info| {
            partition_infos
                .entry(info.kafka_partition_id)
                .and_modify(|existing_info| merge_info(existing_info, &info))
                .or_insert(info);
        });

    let kafka_partition_infos = partition_infos
        .into_iter()
        .map(|(_kafka_partition_id, info)| info)
        .collect();

    GetWriteInfoResponse {
        kafka_partition_infos,
    }
}

// convert the status to a number such that higher numbers are later
// in the data lifecycle
fn status_order(status: KafkaPartitionStatus) -> u8 {
    match status {
        KafkaPartitionStatus::Unspecified => panic!("Unspecified status"),
        KafkaPartitionStatus::Unknown => 0,
        KafkaPartitionStatus::Durable => 1,
        KafkaPartitionStatus::Readable => 2,
        KafkaPartitionStatus::Persisted => 3,
    }
}

fn merge_info(left: &mut KafkaPartitionInfo, right: &KafkaPartitionInfo) {
    info!("existing_info {:?}, info: {:?}", left, right);

    let left_status = left.status();
    let right_status = right.status();

    let new_status = match status_order(left_status).cmp(&status_order(right_status)) {
        std::cmp::Ordering::Less => right_status,
        std::cmp::Ordering::Equal => left_status,
        std::cmp::Ordering::Greater => left_status,
    };

    left.set_status(new_status);
}

/// Runs a query using the flight API on the specified connection
pub async fn run_query(
    sql: impl Into<String>,
    namespace: impl Into<String>,
    querier_connection: Connection,
) -> Vec<RecordBatch> {
    let namespace = namespace.into();
    let sql = sql.into();

    let mut client = influxdb_iox_client::flight::Client::new(querier_connection);

    // This does nothing except test the client handshake implementation.
    client.handshake().await.unwrap();

    let mut response = client
        .perform_query(ReadInfo {
            namespace_name: namespace,
            sql_query: sql,
        })
        .await
        .expect("Error performing query");

    response.collect().await.expect("Error executing query")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_info() {
        #[derive(Debug)]
        struct Test<'a> {
            left: &'a KafkaPartitionInfo,
            right: &'a KafkaPartitionInfo,
            expected: &'a KafkaPartitionInfo,
        }

        let durable = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Durable.into(),
        };

        let readable = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Readable.into(),
        };

        let persisted = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Persisted.into(),
        };

        let unknown = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Unknown.into(),
        };

        let tests = vec![
            Test {
                left: &unknown,
                right: &unknown,
                expected: &unknown,
            },
            Test {
                left: &unknown,
                right: &durable,
                expected: &durable,
            },
            Test {
                left: &unknown,
                right: &readable,
                expected: &readable,
            },
            Test {
                left: &durable,
                right: &unknown,
                expected: &durable,
            },
            Test {
                left: &readable,
                right: &readable,
                expected: &readable,
            },
            Test {
                left: &durable,
                right: &durable,
                expected: &durable,
            },
            Test {
                left: &readable,
                right: &durable,
                expected: &readable,
            },
            Test {
                left: &persisted,
                right: &durable,
                expected: &persisted,
            },
        ];

        for test in tests {
            let mut output = test.left.clone();

            merge_info(&mut output, test.right);
            assert_eq!(
                &output, test.expected,
                "Mismatch\n\nOutput:\n{:#?}\n\nTest:\n{:#?}",
                output, test
            );
        }
    }
}
