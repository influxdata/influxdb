use std::{pin::Pin, sync::Arc};

use futures::Stream;
use observability_deps::tracing::error;
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use tonic::{Request, Response, Streaming};

use arrow_deps::{
    arrow::{
        self,
        array::{make_array, ArrayRef, MutableArrayData},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    arrow_flight::{
        self,
        flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
    },
    datafusion::physical_plan::collect,
};
use data_types::{DatabaseName, DatabaseNameError};
use query::{frontend::sql::SQLQueryPlanner, DatabaseStore};
use server::{ConnectionManager, Server};
use std::fmt::Debug;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {:?} Ticket: {:?}", source, ticket))]
    InvalidTicket {
        source: std::string::FromUtf8Error,
        ticket: Vec<u8>,
    },
    #[snafu(display("Invalid query, could not parse '{}': {}", query, source))]
    InvalidQuery {
        query: String,
        source: serde_json::Error,
    },

    #[snafu(display("Database {} not found", database_name))]
    DatabaseNotFound { database_name: String },

    #[snafu(display(
        "Internal error reading points from database {}:  {}",
        database_name,
        source
    ))]
    Query {
        database_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error planning query {}: {}", query, source))]
    PlanningSQLQuery {
        query: String,
        source: query::frontend::sql::Error,
    },

    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("Invalid RecordBatch: {}", source))]
    InvalidRecordBatch { source: ArrowError },
}

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        error!("Error handling Flight gRPC request: {}", err);
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match &self {
            Self::InvalidTicket { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidQuery { .. } => Status::invalid_argument(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::Query { .. } => Status::internal(self.to_string()),
            Self::PlanningSQLQuery { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidDatabaseName { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidRecordBatch { .. } => Status::internal(self.to_string()),
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

#[derive(Deserialize, Debug)]
/// Body of the `Ticket` serialized and sent to the do_get endpoint; this should
/// be shared with the read API probably...
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

pub fn make_server<M>(server: Arc<Server<M>>) -> FlightServer<impl Flight>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    FlightServer::new(FlightService { server })
}

#[tonic::async_trait]
impl<M: ConnectionManager> Flight for FlightService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    // TODO: Stream results back directly by using `execute` instead of `collect`
    // https://docs.rs/datafusion/3.0.0/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, tonic::Status> {
        let ticket = request.into_inner();
        let json_str = String::from_utf8(ticket.ticket.to_vec()).context(InvalidTicket {
            ticket: ticket.ticket,
        })?;

        let read_info: ReadInfo =
            serde_json::from_str(&json_str).context(InvalidQuery { query: &json_str })?;

        let database = DatabaseName::new(&read_info.database_name).context(InvalidDatabaseName)?;

        let db = self.server.db(&database).context(DatabaseNotFound {
            database_name: &read_info.database_name,
        })?;

        let planner = SQLQueryPlanner::default();
        let executor = self.server.executor();

        let physical_plan = planner
            .query(db, &read_info.sql_query, &executor)
            .await
            .context(PlanningSQLQuery {
                query: &read_info.sql_query,
            })?;

        // execute the query
        let results = collect(Arc::clone(&physical_plan))
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Query {
                database_name: &read_info.database_name,
            })?;
        if results.is_empty() {
            return Err(tonic::Status::internal("There were no results from ticket"));
        }

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema = physical_plan.schema();
        let schema_flight_data =
            arrow_flight::utils::flight_data_from_arrow_schema(schema.as_ref(), &options);

        let mut flights: Vec<Result<FlightData, tonic::Status>> = vec![Ok(schema_flight_data)];

        let mut batches: Vec<Result<FlightData, tonic::Status>> = results
            .iter()
            .map(optimize_record_batch)
            .collect::<Result<Vec<_>, Error>>()?
            .iter()
            .flat_map(|batch| {
                let (flight_dictionaries, flight_batch) =
                    arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);

                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();

        // append batch vector to schema vector, so that the first message sent is the
        // schema
        flights.append(&mut batches);

        let output = futures::stream::iter(flights);

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

// Some batches are small slices of the underlying arrays.
// At this stage we only know the number of rows in the record batch
// and the sizes in bytes of the backing buffers of the column arrays.
// There is no straight-forward relationship between these two quantities,
// since some columns can host variable length data such as strings.
//
// However we can apply a quick&dirty heuristic:
// if the backing buffer is two orders of magnitudes bigger
// than the number of rows in the result set, we assume
// that deep-copying the record batch is cheaper than the and transfer costs.
//
// Possible improvements: take the type of the columns into consideration
// and perhaps sample a few element sizes (taking care of not doing more work
// than to always copying the results in the first place).
//
// Or we just fix this upstream in
// arrow_flight::utils::flight_data_from_arrow_batch and re-encode the array
// into a smaller buffer while we have to copy stuff around anyway.
//
// See rationale and discussions about future improvements on
// https://github.com/influxdata/influxdb_iox/issues/1133
fn optimize_record_batch(batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let max_buf_len = batch
        .columns()
        .iter()
        .map(|a| a.get_array_memory_size())
        .max()
        .unwrap_or_default();

    if max_buf_len > batch.num_rows() * 100 {
        let limited_columns: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| deep_clone_array(batch.column(i)))
            .collect();

        return RecordBatch::try_new(batch.schema(), limited_columns).context(InvalidRecordBatch);
    }
    Ok(batch.clone())
}

fn deep_clone_array(array: &ArrayRef) -> ArrayRef {
    let mut mutable = MutableArrayData::new(vec![array.data()], false, 0);
    mutable.extend(0, 0, array.len());

    make_array(mutable.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_deps::arrow::{
        array::UInt32Array,
        datatypes::{DataType, Field, Schema},
    };
    use arrow_deps::datafusion::physical_plan::limit::truncate_batch;
    use std::sync::Arc;

    #[test]
    fn test_deep_clone_array() {
        let mut builder = UInt32Array::builder(1000);
        builder.append_slice(&[1, 2, 3, 4, 5, 6]).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());
        assert_eq!(array.len(), 6);

        let sliced = array.slice(0, 2);
        assert_eq!(sliced.len(), 2);

        let deep_cloned = deep_clone_array(&sliced);
        assert!(sliced.data().get_array_memory_size() > deep_cloned.data().get_array_memory_size());
    }

    #[test]
    fn test_encode_flight_data() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let mut builder = UInt32Array::builder(1000);
        builder.append_slice(&[1, 2, 3, 4, 5, 6]).unwrap();
        let column: ArrayRef = Arc::new(builder.finish());

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![column])
            .expect("cannot create record batch");

        let (_, baseline_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);

        let big_batch = truncate_batch(&batch, batch.num_rows() - 1);
        let optimized_big_batch = optimize_record_batch(&big_batch).expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = truncate_batch(&batch, 1);
        let optimized_small_batch =
            optimize_record_batch(&small_batch).expect("failed to optimize");
        let (_, optimized_small_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len() > optimized_small_flight_batch.data_body.len()
        );
    }
}
