use crate::{Scenario, GRPC_URL_BASE};
use arrow_deps::{
    arrow::{
        datatypes::Schema,
        ipc::{self, reader},
    },
    arrow_flight::{
        flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, Ticket,
    },
    assert_table_eq,
};
use futures::prelude::*;
use serde::Serialize;
use std::{convert::TryFrom, sync::Arc};

// TODO: this should be shared
#[derive(Serialize, Debug)]
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

pub async fn test(scenario: &Scenario, sql_query: &str, expected_read_data: &[String]) {
    let mut flight_client = FlightServiceClient::connect(GRPC_URL_BASE).await.unwrap();

    let query = ReadInfo {
        database_name: scenario.database_name().into(),
        sql_query: sql_query.into(),
    };

    let t = Ticket {
        ticket: serde_json::to_string(&query).unwrap().into(),
    };
    let mut response = flight_client.do_get(t).await.unwrap().into_inner();

    let flight_data_schema = response.next().await.unwrap().unwrap();
    let schema = Arc::new(Schema::try_from(&flight_data_schema).unwrap());

    let mut dictionaries_by_field = vec![None; schema.fields().len()];

    let mut batches = vec![];

    while let Some(data) = response.next().await {
        let mut data = data.unwrap();
        let mut message =
            ipc::root_as_message(&data.data_header[..]).expect("Error parsing first message");

        while message.header_type() == ipc::MessageHeader::DictionaryBatch {
            reader::read_dictionary(
                &data.data_body,
                message
                    .header_as_dictionary_batch()
                    .expect("Error parsing dictionary"),
                &schema,
                &mut dictionaries_by_field,
            )
            .expect("Error reading dictionary");

            data = response.next().await.unwrap().ok().unwrap();
            message = ipc::root_as_message(&data.data_header[..]).expect("Error parsing message");
        }

        batches.push(
            flight_data_to_arrow_batch(&data, schema.clone(), &dictionaries_by_field)
                .expect("Unable to convert flight data to Arrow batch"),
        );
    }

    assert_table_eq!(expected_read_data, &batches);
}
