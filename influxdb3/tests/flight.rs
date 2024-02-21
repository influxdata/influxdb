use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use futures::TryStreamExt;

use crate::common::TestServer;

mod common;

#[tokio::test]
async fn flight() {
    let server = TestServer::spawn().await;

    // use the influxdb3_client to write in some data
    {
        let client =
            influxdb3_client::Client::new(server.client_addr()).expect("create influxdb3 client");
        client
            .api_v3_write_lp("foo")
            .body(
                "\
                cpu,host=s1,region=us-east usage=0.9 1\n\
                cpu,host=s1,region=us-east usage=0.89 2\n\
                cpu,host=21,region=us-east usage=0.85 3",
            )
            .send()
            .await
            .expect("send write_lp request");
    }

    // Use a FlightClient to communicate via FlightSQL
    let mut client = server.flight_client().await;
    let ticket = Ticket::new(r#"{"database":"foo","sql_query":"SELECT * FROM cpu"}"#);
    let response = client.do_get(ticket).await.expect("send gRPC request");

    // Convert the response to a RecordBatch to make assertions
    let batches: Vec<RecordBatch> = response.try_collect().await.expect("gather record batches");
    let record_batch = batches.first().expect("has first batch");

    assert_eq!(record_batch.num_rows(), 3);
}
