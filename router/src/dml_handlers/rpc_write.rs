mod client;

use super::{DmlHandler, Partitioned};
use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName, TableId};
use dml::{DmlMeta, DmlWrite};
use generated_types::influxdata::iox::ingester::v1::{
    write_service_client::WriteServiceClient, WriteRequest,
};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_pb::encode::encode_write;
use observability_deps::tracing::*;
use sharder::RoundRobin;
use std::{fmt::Debug, time::Duration};
use thiserror::Error;
use trace::ctx::SpanContext;

/// Create a client to the ingester's write service.
pub async fn write_service_client(
    ingester_addr: &str,
) -> WriteServiceClient<client_util::connection::GrpcConnection> {
    let connection = client_util::connection::Builder::default()
        .build(format!("http://{}", ingester_addr))
        .await
        .unwrap_or_else(|e| panic!("failed to connect to server {ingester_addr}: {e}"));
    WriteServiceClient::new(connection.into_grpc_connection())
}

/// The bound on RPC request duration.
///
/// This includes the time taken to send the request, and wait for the response.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors experienced when submitting an RPC write request to an Ingester.
#[derive(Debug, Error)]
pub enum RpcWriteError {
    /// The upstream ingester returned an error response.
    #[error("upstream ingester error: {0}")]
    Upstream(#[from] tonic::Status),

    /// The RPC call timed out after [`RPC_TIMEOUT`] length of time.
    #[error("timeout writing to upstream ingester")]
    Timeout(#[from] tokio::time::error::Elapsed),

    /// A delete request was rejected (not supported).
    #[error("deletes are not supported")]
    DeletesUnsupported,
}

/// A convenience alias for the generated gRPC client.
type GrpcClient = WriteServiceClient<client_util::connection::GrpcConnection>;

/// An [`RpcWrite`] handler submits a write directly to an Ingester via the
/// [gRPC write service].
///
/// Requests are sent to an arbitrary downstream Ingester, and request load is
/// distributed approximately uniformly across all downstream Ingesters. There
/// is no effort made to enforce or attempt data locality.
///
/// # Deletes
///
/// This handler drops delete requests, logging the attempt and returning an
/// error to the client.
///
/// [gRPC write service]: WriteServiceClient
#[derive(Debug)]
pub struct RpcWrite<C = GrpcClient> {
    endpoints: RoundRobin<C>,
}

impl<C> RpcWrite<C> {
    /// Initialise a new [`RpcWrite`] that sends requests to an arbitrary
    /// downstream Ingester, using a round-robin strategy.
    pub fn new(endpoints: RoundRobin<C>) -> Self {
        Self { endpoints }
    }
}

#[async_trait]
impl<C> DmlHandler for RpcWrite<C>
where
    C: client::WriteClient,
{
    type WriteInput = Partitioned<HashMap<TableId, (String, MutableBatch)>>;
    type WriteOutput = Vec<DmlMeta>;

    type WriteError = RpcWriteError;
    type DeleteError = RpcWriteError;

    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        writes: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, RpcWriteError> {
        // Extract the partition key & DML writes.
        let (partition_key, writes) = writes.into_parts();

        // Drop the table names from the value tuple.
        let writes = writes
            .into_iter()
            .map(|(id, (_name, data))| (id, data))
            .collect();

        // Build the DmlWrite
        let op = DmlWrite::new(
            namespace_id,
            writes,
            partition_key.clone(),
            DmlMeta::unsequenced(span_ctx.clone()),
        );

        // Serialise this write into the wire format.
        let req = WriteRequest {
            payload: Some(encode_write(namespace_id.get(), &op)),
        };

        // Perform the gRPC write to an ingester.
        //
        // This includes a dirt simple retry mechanism that WILL need improving
        // (#6173).
        tokio::time::timeout(RPC_TIMEOUT, async {
            loop {
                match self.endpoints.next().write(req.clone()).await {
                    Ok(()) => break,
                    Err(e) => warn!(error=%e, "failed ingester rpc write"),
                };
            }
        })
        .await?;

        debug!(
            %partition_key,
            table_count=op.table_count(),
            %namespace,
            %namespace_id,
            approx_size=%op.size(),
            "dispatched write to ingester"
        );

        Ok(vec![op.meta().clone()])
    }

    async fn delete(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), RpcWriteError> {
        warn!(
            %namespace,
            %namespace_id,
            %table_name,
            "dropping delete request"
        );

        Err(RpcWriteError::DeletesUnsupported)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert_matches::assert_matches;
    use data_types::PartitionKey;

    use super::{client::mock::MockWriteClient, *};

    // Parse `lp` into a table-keyed MutableBatch map.
    pub(crate) fn lp_to_writes(lp: &str) -> HashMap<TableId, (String, MutableBatch)> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");

        writes
            .into_iter()
            .enumerate()
            .map(|(i, (name, data))| (TableId::new(i as _), (name, data)))
            .collect()
    }

    const NAMESPACE_NAME: &str = "bananas";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_write() {
        let batches = lp_to_writes(
            "\
                bananas,tag1=A,tag2=B val=42i 1\n\
                platanos,tag1=A,tag2=B value=42i 2\n\
                another,tag1=A,tag2=B value=42i 3\n\
                bananas,tag1=A,tag2=B val=42i 2\n\
                table,tag1=A,tag2=B val=42i 1\n\
            ",
        );

        // Wrap the table batches in a partition key
        let input = Partitioned::new(PartitionKey::from("2022-01-01"), batches.clone());

        // Init the write handler with a mock client to capture the rpc calls.
        let client = Arc::new(MockWriteClient::default());
        let handler = RpcWrite::new(RoundRobin::new([Arc::clone(&client)]));

        // Drive the RPC writer
        let got = handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await;
        assert_matches!(got, Ok(_));

        // Inspect the resulting RPC call
        let call = {
            let mut calls = client.calls();
            assert_eq!(calls.len(), 1);
            calls.pop().unwrap()
        };

        let payload = assert_matches!(call.payload, Some(p) => p);
        assert_eq!(payload.database_id, NAMESPACE_ID.get());
        assert_eq!(payload.partition_key, "2022-01-01");
        assert_eq!(payload.table_batches.len(), 4);

        let got_tables = payload
            .table_batches
            .into_iter()
            .map(|t| t.table_id)
            .collect::<HashSet<_>>();

        let want_tables = batches
            .into_iter()
            .map(|(id, (_name, _data))| id.get())
            .collect::<HashSet<_>>();

        assert_eq!(got_tables, want_tables);
    }

    #[tokio::test]
    async fn test_write_retries() {
        let batches = lp_to_writes("bananas,tag1=A,tag2=B val=42i 1");

        // Wrap the table batches in a partition key
        let input = Partitioned::new(PartitionKey::from("2022-01-01"), batches.clone());

        // Init the write handler with a mock client to capture the rpc calls.
        let client1 = Arc::new(
            MockWriteClient::default()
                .with_ret([Err(RpcWriteError::Upstream(tonic::Status::internal("")))]),
        );
        let client2 = Arc::new(MockWriteClient::default());
        let handler = RpcWrite::new(RoundRobin::new([
            Arc::clone(&client1),
            Arc::clone(&client2),
        ]));

        // Drive the RPC writer
        let got = handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await;
        assert_matches!(got, Ok(_));

        // Ensure client 2 observed a write.
        let call = {
            let mut calls = client2.calls();
            assert_eq!(calls.len(), 1);
            calls.pop().unwrap()
        };

        let payload = assert_matches!(call.payload, Some(p) => p);
        assert_eq!(payload.database_id, NAMESPACE_ID.get());
        assert_eq!(payload.partition_key, "2022-01-01");
        assert_eq!(payload.table_batches.len(), 1);

        let got_tables = payload
            .table_batches
            .into_iter()
            .map(|t| t.table_id)
            .collect::<HashSet<_>>();

        let want_tables = batches
            .into_iter()
            .map(|(id, (_name, _data))| id.get())
            .collect::<HashSet<_>>();

        assert_eq!(got_tables, want_tables);
    }
}
