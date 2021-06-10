use std::fmt::Debug;
use std::sync::Arc;

use bytes::BytesMut;
use observability_deps::tracing::debug;
use prost::Message;
use tonic::Response;

use data_types::job::Job;
use generated_types::google::FieldViolationExt;
use generated_types::{
    google::{
        longrunning::*,
        protobuf::{Any, Empty},
        rpc::Status,
        FieldViolation, InternalError, NotFound,
    },
    influxdata::iox::management::v1 as management,
    protobuf_type_url,
};
use tracker::{TaskId, TaskStatus, TaskTracker};

use server::{ConnectionManager, Server};
use std::convert::TryInto;

/// Implementation of the write service
struct OperationsService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

pub fn encode_tracker(tracker: TaskTracker<Job>) -> Result<Operation, tonic::Status> {
    let id = tracker.id();
    let is_cancelled = tracker.is_cancelled();
    let status = tracker.get_status();

    let (operation_metadata, is_complete) = match status {
        TaskStatus::Creating => {
            let metadata = management::OperationMetadata {
                job: Some(tracker.metadata().clone().into()),
                ..Default::default()
            };

            (metadata, false)
        }
        TaskStatus::Running {
            total_count,
            pending_count,
            cpu_nanos,
        } => {
            let metadata = management::OperationMetadata {
                cpu_nanos: cpu_nanos as _,
                task_count: total_count as _,
                pending_count: pending_count as _,
                job: Some(tracker.metadata().clone().into()),
                ..Default::default()
            };

            (metadata, false)
        }
        TaskStatus::Complete {
            total_count,
            cpu_nanos,
            wall_nanos,
        } => {
            let metadata = management::OperationMetadata {
                cpu_nanos: cpu_nanos as _,
                task_count: total_count as _,
                wall_nanos: wall_nanos as _,
                job: Some(tracker.metadata().clone().into()),
                ..Default::default()
            };

            (metadata, true)
        }
    };

    let mut buffer = BytesMut::new();
    operation_metadata.encode(&mut buffer).map_err(|error| {
        debug!(?error, "Unexpected error");
        InternalError {}
    })?;

    let metadata = Any {
        type_url: protobuf_type_url(management::OPERATION_METADATA),
        value: buffer.freeze(),
    };

    let result = match (is_complete, is_cancelled) {
        (true, true) => Some(operation::Result::Error(Status {
            code: tonic::Code::Cancelled as _,
            message: "Job cancelled".to_string(),
            details: vec![],
        })),

        (true, false) => Some(operation::Result::Response(Any {
            type_url: "type.googleapis.com/google.protobuf.Empty".to_string(),
            value: Default::default(), // TODO: Verify this is correct
        })),

        _ => None,
    };

    Ok(Operation {
        name: id.to_string(),
        metadata: Some(metadata),
        done: is_complete,
        result,
    })
}

fn get_tracker<M>(server: &Server<M>, tracker: String) -> Result<TaskTracker<Job>, tonic::Status>
where
    M: ConnectionManager + Send + Sync,
{
    let tracker_id = tracker.parse::<TaskId>().map_err(|e| FieldViolation {
        field: "name".to_string(),
        description: e.to_string(),
    })?;

    let tracker = server.get_job(tracker_id).ok_or(NotFound {
        resource_type: "job".to_string(),
        resource_name: tracker,
        ..Default::default()
    })?;

    Ok(tracker)
}

#[tonic::async_trait]
impl<M> operations_server::Operations for OperationsService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn list_operations(
        &self,
        _request: tonic::Request<ListOperationsRequest>,
    ) -> Result<tonic::Response<ListOperationsResponse>, tonic::Status> {
        // TODO: Support pagination
        let operations: Result<Vec<_>, _> = self
            .server
            .tracked_jobs()
            .into_iter()
            .map(encode_tracker)
            .collect();

        Ok(Response::new(ListOperationsResponse {
            operations: operations?,
            next_page_token: Default::default(),
        }))
    }

    async fn get_operation(
        &self,
        request: tonic::Request<GetOperationRequest>,
    ) -> Result<tonic::Response<Operation>, tonic::Status> {
        let request = request.into_inner();
        let tracker = get_tracker(self.server.as_ref(), request.name)?;

        Ok(Response::new(encode_tracker(tracker)?))
    }

    async fn delete_operation(
        &self,
        _: tonic::Request<DeleteOperationRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "IOx does not support operation deletion",
        ))
    }

    async fn cancel_operation(
        &self,
        request: tonic::Request<CancelOperationRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();

        let tracker = get_tracker(self.server.as_ref(), request.name)?;
        tracker.cancel();

        Ok(Response::new(Empty {}))
    }

    async fn wait_operation(
        &self,
        request: tonic::Request<WaitOperationRequest>,
    ) -> Result<tonic::Response<Operation>, tonic::Status> {
        // This should take into account the context deadline timeout
        // Unfortunately these are currently stripped by tonic
        // - https://github.com/hyperium/tonic/issues/75

        let request = request.into_inner();

        let tracker = get_tracker(self.server.as_ref(), request.name)?;
        if let Some(timeout) = request.timeout {
            let timeout = timeout.try_into().field("timeout")?;

            // Timeout is not an error so suppress it
            let _ = tokio::time::timeout(timeout, tracker.join()).await;
        } else {
            tracker.join().await;
        }

        Ok(Response::new(encode_tracker(tracker)?))
    }
}

/// Instantiate the write service
pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> operations_server::OperationsServer<impl operations_server::Operations>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    operations_server::OperationsServer::new(OperationsService { server })
}
