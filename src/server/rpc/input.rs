use tonic::Status;

use generated_types::{
    MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, ReadFilterRequest, ReadGroupRequest, ReadSource,
    ReadWindowAggregateRequest, TagKeysRequest, TagValuesRequest,
};
use query::id::Id;

use std::convert::TryInto;

/// This trait implements extraction of information from all storage gRPC requests. The only method
/// required to implement is `read_source_field` because for some requests the field is named
/// `read_source` and for others it is `tags_source`.
pub trait GrpcInputs {
    fn read_source_field(&self) -> Option<&prost_types::Any>;

    fn read_source_raw(&self) -> Result<&prost_types::Any, Status> {
        Ok(self
            .read_source_field()
            .ok_or_else(|| Status::invalid_argument("missing read_source"))?)
    }

    fn read_source(&self) -> Result<ReadSource, Status> {
        let raw = self.read_source_raw()?;
        let val = &raw.value[..];
        Ok(prost::Message::decode(val).map_err(|_| {
            Status::invalid_argument("value could not be parsed as a ReadSource message")
        })?)
    }

    fn org_id(&self) -> Result<Id, Status> {
        Ok(self
            .read_source()?
            .org_id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u64"))?)
    }

    fn bucket_name(&self) -> Result<String, Status> {
        let bucket: Id = self
            .read_source()?
            .bucket_id
            .try_into()
            .map_err(|_| Status::invalid_argument("bucket_id did not fit in a u64"))?;
        Ok(bucket.to_string())
    }
}

impl GrpcInputs for ReadFilterRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for ReadGroupRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for TagKeysRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

impl GrpcInputs for TagValuesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

impl GrpcInputs for MeasurementNamesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementTagKeysRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementTagValuesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for MeasurementFieldsRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.source.as_ref()
    }
}

impl GrpcInputs for ReadWindowAggregateRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}
