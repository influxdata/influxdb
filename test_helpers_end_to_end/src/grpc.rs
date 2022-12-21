use crate::MiniCluster;
use generated_types::{
    aggregate::AggregateType,
    node::{Comparison, Type as NodeType, Value},
    read_group_request::Group,
    Aggregate, MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, Predicate, ReadFilterRequest, ReadGroupRequest, ReadSource,
    ReadWindowAggregateRequest, TagKeyMetaNames, TagKeysRequest, TagValuesRequest, TimestampRange,
};
use prost::Message;

#[derive(Debug, Default)]
/// Helps create and send influxrpc / gRPC requests to IOx
pub struct GrpcRequestBuilder {
    read_source: Option<generated_types::google::protobuf::Any>,
    range: Option<TimestampRange>,
    predicate: Option<Predicate>,

    // for read_group requests
    group: Option<Group>,
    group_keys: Option<Vec<String>>,
    // also used for read_window_aggregate requests
    aggregate_type: Option<AggregateType>,

    window_every: Option<i64>,
    offset: Option<i64>,
}

impl GrpcRequestBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates the appropriate `Any` protobuf magic for a read source with the cluster's org and
    /// bucket name
    pub fn source(self, cluster: &MiniCluster) -> Self {
        self.explicit_source(cluster.org_id(), cluster.bucket_id())
    }

    /// Creates the appropriate `Any` protobuf magic for a read source with the cluster's org and
    /// bucket name
    pub fn explicit_source(self, org_id: &str, bucket_id: &str) -> Self {
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

    /// Set predicate to be  `tag_name=tag_value` in the horrible gRPC
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
        self.predicate(predicate)
    }

    /// Create a predicate representing _f=field_name in the horrible gRPC structs
    pub fn field_predicate(self, field_name: impl Into<String>) -> Self {
        let predicate = Predicate {
            root: Some(Node {
                node_type: NodeType::ComparisonExpression as i32,
                children: vec![
                    Node {
                        node_type: NodeType::TagRef as i32,
                        children: vec![],
                        value: Some(Value::TagRefValue([255].to_vec())),
                    },
                    Node {
                        node_type: NodeType::Literal as i32,
                        children: vec![],
                        value: Some(Value::StringValue(field_name.into())),
                    },
                ],
                value: Some(Value::Comparison(Comparison::Equal as _)),
            }),
        };
        self.predicate(predicate)
    }

    /// Create a predicate representing _m=measurement_name in the horrible gRPC structs
    pub fn measurement_predicate(self, measurement_name: impl Into<String>) -> Self {
        let predicate = Predicate {
            root: Some(Node {
                node_type: NodeType::ComparisonExpression as i32,
                children: vec![
                    Node {
                        node_type: NodeType::TagRef as i32,
                        children: vec![],
                        value: Some(Value::TagRefValue([00].to_vec())),
                    },
                    Node {
                        node_type: NodeType::Literal as i32,
                        children: vec![],
                        value: Some(Value::StringValue(measurement_name.into())),
                    },
                ],
                value: Some(Value::Comparison(Comparison::Equal as _)),
            }),
        };
        self.predicate(predicate)
    }

    /// Set predicate to tag_name ~= /pattern/
    pub fn regex_match_predicate(
        self,
        tag_key_name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        self.regex_predicate(tag_key_name, pattern, Comparison::Regex)
    }

    /// Set predicate to tag_name !~ /pattern/
    pub fn not_regex_match_predicate(
        self,
        tag_key_name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        self.regex_predicate(tag_key_name, pattern, Comparison::NotRegex)
    }

    /// Set predicate to `tag_name <op> /pattern/`
    ///
    /// where op is `Regex` or `NotRegEx`
    /// The constitution of this request was formed by looking at a real request
    /// made to storage, which looked like this:
    ///
    /// ```text
    /// root:<
    ///         node_type:COMPARISON_EXPRESSION
    ///         children:<node_type:TAG_REF tag_ref_value:"tag_key_name" >
    ///         children:<node_type:LITERAL regex_value:"pattern" >
    ///         comparison:REGEX
    /// >
    /// ```
    pub fn regex_predicate(
        self,
        tag_key_name: impl Into<String>,
        pattern: impl Into<String>,
        comparison: Comparison,
    ) -> Self {
        let predicate = Predicate {
            root: Some(Node {
                node_type: NodeType::ComparisonExpression as i32,
                children: vec![
                    Node {
                        node_type: NodeType::TagRef as i32,
                        children: vec![],
                        value: Some(Value::TagRefValue(tag_key_name.into().into())),
                    },
                    Node {
                        node_type: NodeType::Literal as i32,
                        children: vec![],
                        value: Some(Value::RegexValue(pattern.into())),
                    },
                ],
                value: Some(Value::Comparison(comparison as _)),
            }),
        };
        self.predicate(predicate)
    }

    /// Set the predicate being crated
    pub fn predicate(self, predicate: Predicate) -> Self {
        assert!(self.predicate.is_none(), "Overwriting existing predicate");
        Self {
            predicate: Some(predicate),
            ..self
        }
    }

    /// Append the specified strings to the `group_keys` request
    pub fn group_keys<'a>(self, group_keys: impl IntoIterator<Item = &'a str>) -> Self {
        assert!(self.group_keys.is_none(), "Overwriting existing group_keys");
        let group_keys = group_keys.into_iter().map(|s| s.to_string()).collect();
        Self {
            group_keys: Some(group_keys),
            ..self
        }
    }

    /// set the specified grouping method on a read_group request
    pub fn group(self, group: Group) -> Self {
        assert!(self.group.is_none(), "Overwriting existing group");
        Self {
            group: Some(group),
            ..self
        }
    }

    /// set the specified grouping grouping aggregate on a read_group or read_window_aggregate request
    pub fn aggregate_type(self, aggregate_type: AggregateType) -> Self {
        assert!(
            self.aggregate_type.is_none(),
            "Overwriting existing aggregate"
        );
        Self {
            aggregate_type: Some(aggregate_type),
            ..self
        }
    }

    /// set the window_every field for a read_window_aggregate request
    pub fn window_every(self, window_every: i64) -> Self {
        assert!(
            self.window_every.is_none(),
            "Overwriting existing window_every"
        );
        Self {
            window_every: Some(window_every),
            ..self
        }
    }

    /// set the offset field for a read_window_aggregate request
    pub fn offset(self, offset: i64) -> Self {
        assert!(self.offset.is_none(), "Overwriting existing offset");
        Self {
            offset: Some(offset),
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

    /// Creates a read group request
    pub fn build_read_group(self) -> tonic::Request<ReadGroupRequest> {
        let aggregate = self.aggregate_type.map(|aggregate_type| Aggregate {
            r#type: aggregate_type.into(),
        });

        let group_keys = self.group_keys.unwrap_or_default();
        let group = self
            .group
            .expect("no group specified, can't create read_group_request")
            .into();

        tonic::Request::new(ReadGroupRequest {
            read_source: self.read_source,
            range: self.range,
            predicate: self.predicate,
            group_keys,
            group,
            aggregate,
        })
    }

    /// Creates a read window_aggregate request
    pub fn build_read_window_aggregate(self) -> tonic::Request<ReadWindowAggregateRequest> {
        // we support only a single aggregate for now
        let aggregate = self
            .aggregate_type
            .map(|aggregate_type| {
                vec![Aggregate {
                    r#type: aggregate_type.into(),
                }]
            })
            .expect("No aggregate specified, can't create read_window_aggregate request");

        tonic::Request::new(ReadWindowAggregateRequest {
            read_source: self.read_source,
            range: self.range,
            predicate: self.predicate,
            window_every: self.window_every.expect("no window_every specified"),
            offset: self.offset.expect("no offset specified"),
            aggregate,
            window: None,
            tag_key_meta_names: TagKeyMetaNames::Text as i32,
        })
    }
}
