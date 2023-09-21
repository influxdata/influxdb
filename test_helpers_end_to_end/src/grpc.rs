use crate::MiniCluster;
use generated_types::{
    aggregate::AggregateType,
    node::{Comparison, Logical, Type as NodeType, Value},
    read_group_request::Group,
    Aggregate, MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, Predicate, ReadFilterRequest, ReadGroupRequest, ReadSource,
    ReadWindowAggregateRequest, TagKeyMetaNames, TagKeysRequest, TagValuesRequest, TimestampRange,
};
use prost::Message;

#[derive(Debug, Default, Clone)]
/// Helps create and send influxrpc / gRPC requests to IOx
pub struct GrpcRequestBuilder {
    read_source: Option<generated_types::google::protobuf::Any>,
    range: Option<TimestampRange>,
    pub predicate: Option<Predicate>,

    // for read_group requests
    group: Option<Group>,
    group_keys: Option<Vec<String>>,
    // also used for read_window_aggregate requests
    aggregate_type: Option<AggregateType>,

    window_every: Option<i64>,
    offset: Option<i64>,
}

/// Trait for converting various literal rust values to their
/// corresponding Nodes in GRPC
pub trait GrpcLiteral {
    fn make_node(&self) -> Node;
}

impl GrpcLiteral for f64 {
    fn make_node(&self) -> Node {
        Node {
            node_type: NodeType::Literal.into(),
            children: vec![],
            value: Some(Value::FloatValue(*self)),
        }
    }
}

impl GrpcLiteral for i64 {
    fn make_node(&self) -> Node {
        Node {
            node_type: NodeType::Literal.into(),
            children: vec![],
            value: Some(Value::IntValue(*self)),
        }
    }
}

impl GrpcLiteral for &str {
    fn make_node(&self) -> Node {
        string_value_node(*self)
    }
}

impl GrpcLiteral for &String {
    fn make_node(&self) -> Node {
        string_value_node(self.as_str())
    }
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

    /// Add `tag_name=tag_value` to the predicate in the horrible gRPC structs
    pub fn tag_predicate(self, tag_name: impl Into<String>, tag_value: impl Into<String>) -> Self {
        self.tag_comparison_predicate(tag_name, tag_value, Comparison::Equal)
    }

    /// For all `(tag_name, tag_value)` pairs, add `tag_name=tag_value OR tag_name=tag_value ...`
    /// to the predicate.
    pub fn or_tag_predicates(
        self,
        tags: impl Iterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        tags.into_iter().fold(self, |acc, (tag_name, tag_value)| {
            let node = comparison_expression_node(
                tag_ref_node(tag_name.into()),
                Comparison::Equal,
                string_value_node(tag_value),
            );
            acc.combine_predicate(Logical::Or, node)
        })
    }

    /// Add `tag_name!=tag_value` to the predicate in the horrible gRPC structs
    pub fn not_tag_predicate(
        self,
        tag_name: impl Into<String>,
        tag_value: impl Into<String>,
    ) -> Self {
        self.tag_comparison_predicate(tag_name, tag_value, Comparison::NotEqual)
    }

    /// Add `tag_name <op> value` to the predicate where `<op>` is `Equal` or `NotEqual`
    fn tag_comparison_predicate(
        self,
        tag_name: impl Into<String>,
        tag_value: impl Into<String>,
        comparison: Comparison,
    ) -> Self {
        let node = comparison_expression_node(
            tag_ref_node(tag_name.into()),
            comparison,
            string_value_node(tag_value),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `tag_name1=tag_name2` to the predicate
    pub fn tag_to_tag_predicate(
        self,
        tag_name1: impl Into<String>,
        tag_name2: impl Into<String>,
    ) -> Self {
        let node = comparison_expression_node(
            tag_ref_node(tag_name1.into()),
            Comparison::Equal,
            tag_ref_node(tag_name2.into()),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `_value=val` to the predicate in the horrible gRPC structs
    pub fn field_value_predicate<L: GrpcLiteral>(self, l: L) -> Self {
        let node =
            comparison_expression_node(field_ref_node("_value"), Comparison::Equal, l.make_node());
        self.combine_predicate(Logical::And, node)
    }

    /// For all values, add `_value=val OR _value=val ...` to the predicate.
    pub fn or_field_value_predicates<L: GrpcLiteral>(
        self,
        values: impl Iterator<Item = L>,
    ) -> Self {
        values.into_iter().fold(self, |acc, value| {
            let node = comparison_expression_node(
                field_ref_node("_value"),
                Comparison::Equal,
                value.make_node(),
            );
            acc.combine_predicate(Logical::Or, node)
        })
    }

    /// Add `_f=field_name` to the predicate in the horrible gRPC structs
    pub fn field_predicate(self, field_name: impl Into<String>) -> Self {
        let node = comparison_expression_node(
            tag_ref_node([255].to_vec()),
            Comparison::Equal,
            string_value_node(field_name),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `<lit>=<lit>` to the predicate in the horrible gRPC structs
    pub fn lit_lit_predicate<L1: GrpcLiteral, L2: GrpcLiteral>(self, l1: L1, l2: L2) -> Self {
        let node = comparison_expression_node(l1.make_node(), Comparison::Equal, l2.make_node());
        self.combine_predicate(Logical::And, node)
    }

    /// Add `_f!=field_name` to the predicate in the horrible gRPC structs
    pub fn not_field_predicate(self, field_name: impl Into<String>) -> Self {
        let node = comparison_expression_node(
            tag_ref_node([255].to_vec()),
            Comparison::NotEqual,
            string_value_node(field_name),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `_m=measurement_name` to the predicate in the horrible gRPC structs
    pub fn measurement_predicate(self, measurement_name: impl Into<String>) -> Self {
        let node = comparison_expression_node(
            tag_ref_node([00].to_vec()),
            Comparison::Equal,
            string_value_node(measurement_name),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `_m!=measurement_name` to the predicate in the horrible gRPC structs
    pub fn not_measurement_predicate(self, measurement_name: impl Into<String>) -> Self {
        let node = comparison_expression_node(
            tag_ref_node([00].to_vec()),
            Comparison::NotEqual,
            string_value_node(measurement_name),
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Add `tag_name ~= /pattern/` to the predicate
    pub fn regex_match_predicate(
        self,
        tag_name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        self.regex_predicate(tag_name, pattern, Comparison::Regex)
    }

    /// Add `tag_name !~ /pattern/` to the predicate
    pub fn not_regex_match_predicate(
        self,
        tag_name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        self.regex_predicate(tag_name, pattern, Comparison::NotRegex)
    }

    /// Add `tag_name <op> /pattern/` to the predicate, where op is `Regex` or `NotRegEx`.
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
    fn regex_predicate(
        self,
        tag_name: impl Into<String>,
        pattern: impl Into<String>,
        comparison: Comparison,
    ) -> Self {
        let node = comparison_expression_node(
            tag_ref_node(tag_name.into()),
            comparison,
            Node {
                node_type: NodeType::Literal as i32,
                children: vec![],
                value: Some(Value::RegexValue(pattern.into())),
            },
        );
        self.combine_predicate(Logical::And, node)
    }

    /// Set the predicate being created, panicking if this would overwrite an existing predicate
    pub fn predicate(self, predicate: Predicate) -> Self {
        assert!(self.predicate.is_none(), "Overwriting existing predicate");
        Self {
            predicate: Some(predicate),
            ..self
        }
    }

    /// Combine any existing predicate with the specified logical operator and node. If there is no
    /// existing predicate, set the predicate to only the specified node.
    pub fn combine_predicate(mut self, operator: Logical, new_node: Node) -> Self {
        let old_predicate = self.predicate.take();

        let combined_predicate = match old_predicate {
            Some(Predicate {
                root: Some(old_node),
            }) => Predicate {
                root: Some(Node {
                    node_type: NodeType::LogicalExpression as i32,
                    children: vec![old_node, new_node],
                    value: Some(Value::Logical(operator as i32)),
                }),
            },
            _ => Predicate {
                root: Some(new_node),
            },
        };

        Self {
            predicate: Some(combined_predicate),
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

    /// Set the specified grouping method on a read_group request
    pub fn group(self, group: Group) -> Self {
        assert!(self.group.is_none(), "Overwriting existing group");
        Self {
            group: Some(group),
            ..self
        }
    }

    /// Set the specified grouping grouping aggregate on a read_group or read_window_aggregate
    /// request
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

    /// Set the window_every field for a read_window_aggregate request
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

    /// Set the offset field for a read_window_aggregate request
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
            predicate: self.predicate,
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

pub fn field_ref_node(field_name: impl Into<String>) -> Node {
    Node {
        node_type: NodeType::FieldRef.into(),
        children: vec![],
        value: Some(Value::FieldRefValue(field_name.into())),
    }
}

pub fn tag_ref_node(tag_name: impl Into<Vec<u8>>) -> Node {
    Node {
        node_type: NodeType::TagRef as i32,
        children: vec![],
        value: Some(Value::TagRefValue(tag_name.into())),
    }
}

pub fn string_value_node(value: impl Into<String>) -> Node {
    Node {
        node_type: NodeType::Literal as i32,
        children: vec![],
        value: Some(Value::StringValue(value.into())),
    }
}

pub fn comparison_expression_node(lhs: Node, comparison: Comparison, rhs: Node) -> Node {
    Node {
        node_type: NodeType::ComparisonExpression as i32,
        children: vec![lhs, rhs],
        value: Some(Value::Comparison(comparison as _)),
    }
}
