use crate::MiniCluster;
use generated_types::{
    node::{Comparison, Type as NodeType, Value},
    MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, Predicate, ReadFilterRequest, ReadSource, TagKeysRequest,
    TagValuesRequest, TimestampRange,
};
use prost::Message;

pub struct GrpcRequestBuilder {
    read_source: Option<generated_types::google::protobuf::Any>,
    range: Option<TimestampRange>,
    predicate: Option<Predicate>,
}

impl Default for GrpcRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcRequestBuilder {
    pub fn new() -> Self {
        Self {
            read_source: None,
            range: None,
            predicate: None,
        }
    }

    /// Creates the appropriate `Any` protobuf magic for a read source with the cluster's org and
    /// bucket name
    pub fn source(self, cluster: &MiniCluster) -> Self {
        let org_id = cluster.org_id();
        let bucket_id = cluster.bucket_id();
        let org_id = u64::from_str_radix(org_id, 16).unwrap();
        let bucket_id = u64::from_str_radix(bucket_id, 16).unwrap();

        let partition_id = u64::from(u32::MAX);
        let read_source = ReadSource {
            org_id,
            bucket_id,
            partition_id,
        };

        // Do the magic to-any conversion
        let mut d = bytes::BytesMut::new();
        read_source.encode(&mut d).unwrap();
        let read_source = generated_types::google::protobuf::Any {
            type_url: "/TODO".to_string(),
            value: d.freeze(),
        };

        Self {
            read_source: Some(read_source),
            ..self
        }
    }

    pub fn timestamp_range(self, start: i64, end: i64) -> Self {
        Self {
            range: Some(TimestampRange { start, end }),
            ..self
        }
    }

    /// Create a predicate representing tag_name=tag_value in the horrible gRPC
    /// structs
    pub fn tag_predicate(self, tag_name: impl Into<String>, tag_value: impl Into<String>) -> Self {
        let predicate = Predicate {
            root: Some(Node {
                node_type: NodeType::ComparisonExpression as i32,
                children: vec![
                    Node {
                        node_type: NodeType::TagRef as i32,
                        children: vec![],
                        value: Some(Value::TagRefValue(tag_name.into().into())),
                    },
                    Node {
                        node_type: NodeType::Literal as i32,
                        children: vec![],
                        value: Some(Value::StringValue(tag_value.into())),
                    },
                ],
                value: Some(Value::Comparison(Comparison::Equal as _)),
            }),
        };
        Self {
            predicate: Some(predicate),
            ..self
        }
    }

    pub fn build_read_filter(self) -> tonic::Request<ReadFilterRequest> {
        tonic::Request::new(ReadFilterRequest {
            read_source: self.read_source,
            range: self.range,
            predicate: self.predicate,
            ..Default::default()
        })
    }

    pub fn build_tag_keys(self) -> tonic::Request<TagKeysRequest> {
        tonic::Request::new(TagKeysRequest {
            tags_source: self.read_source,
            range: self.range,
            predicate: self.predicate,
        })
    }

    pub fn build_tag_values(self, tag_key: &str) -> tonic::Request<TagValuesRequest> {
        tonic::Request::new(TagValuesRequest {
            tags_source: self.read_source,
            range: self.range,
            predicate: self.predicate,
            tag_key: tag_key.as_bytes().to_vec(),
        })
    }

    pub fn build_measurement_names(self) -> tonic::Request<MeasurementNamesRequest> {
        tonic::Request::new(MeasurementNamesRequest {
            source: self.read_source,
            range: self.range,
            predicate: None,
        })
    }

    pub fn build_measurement_tag_keys(
        self,
        measurement: &str,
    ) -> tonic::Request<MeasurementTagKeysRequest> {
        tonic::Request::new(MeasurementTagKeysRequest {
            source: self.read_source,
            measurement: measurement.to_string(),
            range: self.range,
            predicate: self.predicate,
        })
    }

    pub fn build_measurement_tag_values(
        self,
        measurement: &str,
        tag_key: &str,
    ) -> tonic::Request<MeasurementTagValuesRequest> {
        tonic::Request::new(MeasurementTagValuesRequest {
            source: self.read_source,
            measurement: measurement.to_string(),
            tag_key: tag_key.to_string(),
            range: self.range,
            predicate: self.predicate,
        })
    }

    pub fn build_measurement_fields(
        self,
        measurement: &str,
    ) -> tonic::Request<MeasurementFieldsRequest> {
        tonic::Request::new(MeasurementFieldsRequest {
            source: self.read_source,
            measurement: measurement.to_string(),
            range: self.range,
            predicate: self.predicate,
        })
    }
}
