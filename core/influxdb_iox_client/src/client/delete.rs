use client_util::connection::GrpcConnection;

use self::generated_types::{delete_service_client::DeleteServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::delete::v1::*;
    pub use generated_types::influxdata::iox::predicate::v1::*;
}

/// An IOx Delete API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     delete::{
///         Client,
///         generated_types::*,
///     },
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // Delete some data
/// let pred = Predicate {
///     range: Some(TimestampRange {
///         start: 100,
///         end: 120,
///     }),
///     exprs: vec![Expr {
///         column: String::from("region"),
///         op: Op::Eq.into(),
///         scalar: Some(Scalar {
///             value: Some(scalar::Value::ValueString(
///                 String::from("west"),
///             )),
///         }),
///     }],
/// };
/// client
///     .delete(
///         42,
///         "my_table",
///         pred,
///     )
///     .await
///     .expect("failed to delete data");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: DeleteServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: DeleteServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Delete data from a table on a specified predicate
    pub async fn delete(
        &mut self,
        database_id: i64,
        table_name: impl Into<String> + Send,
        predicate: Predicate,
    ) -> Result<(), Error> {
        let table_name = table_name.into();

        self.inner
            .delete(DeleteRequest {
                payload: Some(DeletePayload {
                    database_id,
                    table_name,
                    predicate: Some(predicate),
                }),
            })
            .await?;

        Ok(())
    }
}
