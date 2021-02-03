use super::rpc::GrpcService;

use arrow_deps::{
    arrow,
    arrow_flight::{
        self, flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
        FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
        SchemaResult, Ticket,
    },
    datafusion::physical_plan::collect,
};
use futures::Stream;
use query::{frontend::sql::SQLQueryPlanner, DatabaseStore};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::pin::Pin;
use tonic::{Request, Response, Streaming};
use tracing::error;

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

#[tonic::async_trait]
impl<T> FlightService for GrpcService<T>
where
    T: DatabaseStore + 'static,
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

        let db = self
            .db_store
            .db(&read_info.database_name)
            .await
            .context(DatabaseNotFound {
                database_name: &read_info.database_name,
            })?;

        let planner = SQLQueryPlanner::default();
        let executor = self.db_store.executor();

        let physical_plan = planner
            .query(&*db, &read_info.sql_query, &executor)
            .await
            .context(PlanningSQLQuery {
                query: &read_info.sql_query,
            })?;

        // execute the query
        let results = collect(physical_plan.clone())
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
            .flat_map(|batch| {
                let (flight_dictionaries, flight_batch) =
                    arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);
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
