//! This module has logic to translate gRPC structures into the native
//! storage system form by extending the builders for those structures with new
//! traits
//!
//! RPCPredicate --> query::Predicates
//!
//! Aggregates / windows --> query::GroupByAndAggregate
use std::{convert::TryFrom, fmt};

use datafusion::{
    logical_plan::{binary_expr, Expr, Operator},
    prelude::*,
    scalar::ScalarValue,
};
use generated_types::{
    aggregate::AggregateType as RPCAggregateType, node::Comparison as RPCComparison,
    node::Logical as RPCLogical, node::Value as RPCValue, read_group_request::Group as RPCGroup,
    Aggregate as RPCAggregate, Duration as RPCDuration, Node as RPCNode, Predicate as RPCPredicate,
    Window as RPCWindow,
};

use super::{TAG_KEY_FIELD, TAG_KEY_MEASUREMENT};
use observability_deps::tracing::warn;
use query::func::regex;
use query::group_by::{Aggregate as QueryAggregate, WindowDuration};
use query::predicate::PredicateBuilder;
use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating aggregate: Exactly one aggregate is supported, but {} were supplied: {:?}",
                    aggregates.len(), aggregates))]
    AggregateNotSingleton { aggregates: Vec<RPCAggregate> },

    #[snafu(display("Error creating aggregate: Unknown aggregate type {}", aggregate_type))]
    UnknownAggregate { aggregate_type: i32 },

    #[snafu(display("Error creating aggregate: Unknown group type: {}", group_type))]
    UnknownGroup { group_type: i32 },

    #[snafu(display(
        "Incompatible read_group request: Group::None had {} group keys (expected 0)",
        num_group_keys
    ))]
    InvalidGroupNone { num_group_keys: usize },

    #[snafu(display("Error creating predicate: Unexpected empty predicate: Node"))]
    EmptyPredicateNode {},

    #[snafu(display("Error creating predicate: Unexpected empty predicate value"))]
    EmptyPredicateValue {},

    #[snafu(display("Error parsing window bounds: No window specified"))]
    EmptyWindow {},

    #[snafu(display("Error parsing window bounds duration 'window.every': {}", description))]
    InvalidWindowEveryDuration { description: String },

    #[snafu(display(
        "Error parsing window bounds duration 'window.offset': {}",
        description
    ))]
    InvalidWindowOffsetDuration { description: String },

    #[snafu(display("Internal error: found measurement tag reference in unexpected location"))]
    InternalInvalidMeasurementReference {},

    #[snafu(display("Internal error: found field tag reference in unexpected location"))]
    InternalInvalidFieldReference {},

    #[snafu(display("Invalid regex pattern"))]
    RegExpPatternInvalid {},

    #[snafu(display("Internal error: regex expression input not expected"))]
    InternalInvalidRegexExprReference {},

    #[snafu(display("Internal error: incorrect number of nodes: {:?}", num_children))]
    InternalInvalidRegexExprChildren { num_children: usize },

    #[snafu(display("Error creating predicate: StartsWith comparisons not supported"))]
    StartsWithNotSupported {},

    #[snafu(display(
        "Error creating predicate: Unexpected children for predicate: {:?}",
        value
    ))]
    UnexpectedChildren { value: RPCValue },

    #[snafu(display("Error creating predicate: Unknown logical node type: {}", logical))]
    UnknownLogicalNode { logical: i32 },

    #[snafu(display(
        "Error creating predicate: Unknown comparison node type: {}",
        comparison
    ))]
    UnknownComparisonNode { comparison: i32 },

    #[snafu(display(
        "Error creating predicate: Unsupported number of children in binary operator {:?}: {} (must be 2)",
        op,
        num_children
    ))]
    UnsupportedNumberOfChildren { op: Operator, num_children: usize },

    #[snafu(display("Error converting tag_name to utf8: {}", source))]
    ConvertingTagName { source: std::string::FromUtf8Error },

    #[snafu(display("Error converting field_name to utf8: {}", source))]
    ConvertingFieldName { source: std::string::FromUtf8Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Defines the different ways series can be grouped and aggregated
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByAndAggregate {
    /// group by a set of (Tag) columns, applying an agg to each field
    ///
    /// The resulting data is ordered so that series with the same
    /// values in `group_columns` appear contiguously.
    Columns {
        agg: QueryAggregate,
        group_columns: Vec<String>,
    },

    /// Group by a "window" in time, applying agg to each field
    ///
    /// The window is defined in terms three values:
    ///
    /// time: timestamp
    /// every: Duration
    /// offset: Duration
    ///
    /// The bounds are then calculated at a high level by
    /// bounds = truncate((time_column_reference + offset), every)
    ///
    /// Where the truncate function is different depending on the
    /// specific Duration
    ///
    /// This structure is different than the input (typically from gRPC)
    /// and the underyling calculation (in window.rs), so that we can do
    /// the input validation checking when creating this structure (rather
    /// than in window.rs). The alternate would be to pass the structure
    /// more directly from gRPC to window.rs, which would require less
    /// translation but more error checking in window.rs.
    Window {
        agg: QueryAggregate,
        every: WindowDuration,
        offset: WindowDuration,
    },
}

/// A trait for adding gRPC specific nodes to the generic predicate builder
pub trait AddRpcNode
where
    Self: Sized,
{
    fn rpc_predicate(self, predicate: Option<RPCPredicate>) -> Result<Self>;
}

impl AddRpcNode for PredicateBuilder {
    /// Adds the predicates represented by the Node (predicate tree)
    /// into predicates that can be evaluted by the storage system
    ///
    /// RPC predicates can have several different types of 'predicate' embedded
    /// in them.
    ///
    /// Predicates on tag value (where a tag is a column)
    ///
    /// Predicates on field value (where field is also a column)
    ///
    /// Predicates on 'measurement name' (encoded as tag_ref=\x00), aka select
    /// from a particular table
    ///
    /// Predicates on 'field name' (encoded as tag_ref=\xff), aka select only
    /// specific fields
    ///
    /// This code pulls apart the predicates, if any, into a StoragePredicate
    /// that breaks the predicate apart
    fn rpc_predicate(self, rpc_predicate: Option<RPCPredicate>) -> Result<Self> {
        match rpc_predicate {
            // no input predicate, is fine
            None => Ok(self),
            Some(rpc_predicate) => {
                match rpc_predicate.root {
                    None => EmptyPredicateNode {}.fail(),
                    Some(node) => {
                        // normalize so the rest of the passes can deal with fewer cases
                        let node = normalize_node(node)?;

                        // step one is to flatten any AND tree into a vector of conjucts
                        let conjuncts = flatten_ands(node, Vec::new())?;
                        conjuncts.into_iter().try_fold(self, convert_simple_node)
                    }
                }
            }
        }
    }
}

/// cleans up / normalizes the input in preparation for other
/// processing. Noramlizations performed:
///
/// 1. Flatten `None` value nodes with `children` of length 1 (semantically the
/// same as the child itself). Specifically, if the input is:
///
/// ```
/// Node {
///  value: None,
///  children: [child],
/// }
/// ```
///
/// Then the output is:
///
/// ```
/// child
/// ```
fn normalize_node(node: RPCNode) -> Result<RPCNode> {
    let RPCNode {
        node_type,
        children,
        value,
    } = node;

    let mut normalized_children = children
        .into_iter()
        .map(normalize_node)
        .collect::<Result<Vec<_>>>()?;

    match (value, normalized_children.len()) {
        // Sometimes InfluxQL sends in a RPCNode with 1 child and no value
        // which seems some sort of wrapper -- unwrap this case
        (None, 1) => Ok(normalized_children.pop().unwrap()),
        // It is not clear what None means without exactly one child..
        (None, _) => EmptyPredicateValue {}.fail(),
        (Some(value), _) => {
            // performance any other normalizations needed
            Ok(RPCNode {
                node_type,
                children: normalized_children,
                value: Some(value),
            })
        }
    }
}

/// Converts the node and updates the `StoragePredicate` being built, as
/// appropriate
///
/// It recognizes special predicate patterns and pulls them into
/// the fields on `StoragePredicate` for special processing. If no
/// patterns are matched, it falls back to a generic DataFusion Expr
fn convert_simple_node(builder: PredicateBuilder, node: RPCNode) -> Result<PredicateBuilder> {
    if let Ok(in_list) = InList::try_from(&node) {
        let InList { lhs, value_list } = in_list;

        // look for tag or measurement = <values>
        if let Some(RPCValue::TagRefValue(tag_name)) = lhs.value {
            if tag_name.is_measurement() {
                // add the table names as a predicate
                return Ok(builder.tables(value_list));
            } else if tag_name.is_field() {
                return Ok(builder.field_columns(value_list));
            }
        }
    }

    // If no special case applies, fall back to generic conversion
    let expr = convert_node_to_expr(node)?;

    Ok(builder.add_expr(expr))
}

/// converts a tree of (a AND (b AND c)) into [a, b, c]
fn flatten_ands(node: RPCNode, mut dst: Vec<RPCNode>) -> Result<Vec<RPCNode>> {
    // try to break it up, if possible
    if Some(RPCValue::Logical(RPCLogical::And as i32)) == node.value {
        let RPCNode { children, .. } = node;
        // try and add each child separately
        for child in children {
            dst = flatten_ands(child, dst)?;
        }
    } else {
        dst.push(node);
    }

    Ok(dst)
}

// Represents a predicate like <expr> IN (option1, option2, option3, ....)
//
// use `try_from_node1 to convert a tree like as ((expr = option1) OR (expr =
// option2)) or (expr = option3)) ... into such a form
#[derive(Debug)]
struct InList {
    lhs: RPCNode,
    value_list: Vec<String>,
}

impl TryFrom<&RPCNode> for InList {
    type Error = &'static str;

    /// If node represents an OR tree like (expr = option1) OR (expr=option2)...
    /// extracts an InList like expr IN (option1, option2)
    fn try_from(node: &RPCNode) -> Result<Self, &'static str> {
        InListBuilder::default().append(node)?.build()
    }
}

impl InList {
    fn new(lhs: RPCNode) -> Self {
        Self {
            lhs,
            value_list: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct InListBuilder {
    inner: Option<InList>,
}

impl InListBuilder {
    /// given we are converting and expression like (self) OR (rhs)
    ///
    /// attempts to flatten rhs into self
    ///
    /// For example, if we are at self OR (foo = 'bar') and self.lhs
    /// is foo, will add 'bar' to value_list
    fn append(self, node: &RPCNode) -> Result<Self, &'static str> {
        // lhs = rhs
        if Some(RPCValue::Comparison(RPCComparison::Equal as i32)) == node.value {
            assert_eq!(node.children.len(), 2);
            let lhs = &node.children[0];
            let rhs = &node.children[1];
            self.append_equal(lhs, rhs)
        }
        // lhs OR rhs
        else if Some(RPCValue::Logical(RPCLogical::Or as i32)) == node.value {
            assert_eq!(node.children.len(), 2);

            let lhs = &node.children[0];
            let rhs = &node.children[1];

            // recurse down both sides
            self.append(lhs).and_then(|s| s.append(rhs))
        } else {
            Err("Found something other than equal or OR")
        }
    }

    // append lhs = rhs expression, if possible, return None if not
    fn append_equal(mut self, lhs: &RPCNode, rhs: &RPCNode) -> Result<Self, &'static str> {
        let mut in_list = self
            .inner
            .take()
            .unwrap_or_else(|| InList::new(lhs.clone()));

        // lhs = rhs as String
        if let Some(RPCValue::StringValue(string_value)) = &rhs.value {
            if &in_list.lhs == lhs {
                in_list.value_list.push(string_value.clone());
                self.inner = Some(in_list);
                Ok(self)
            } else {
                Err("lhs did not match")
            }
        } else {
            Err("rhs wasn't a string")
        }
    }

    // consume self and return the built InList
    fn build(self) -> Result<InList, &'static str> {
        self.inner.ok_or("No sub expressions found")
    }
}

// encodes the magic special bytes that the storage gRPC layer uses to
// encode measurement name and field name as tag
pub trait SpecialTagKeys {
    /// Return true if this tag key actually refers to a measurement
    /// name (e.g. _measurement or _m)
    fn is_measurement(&self) -> bool;

    /// Return true if this tag key actually refers to a field
    /// name (e.g. _field or _f)
    fn is_field(&self) -> bool;
}

impl SpecialTagKeys for Vec<u8> {
    fn is_measurement(&self) -> bool {
        self.as_slice() == TAG_KEY_MEASUREMENT
    }

    /// Return true if this tag key actually refers to a field
    /// name (e.g. _field or _f)
    fn is_field(&self) -> bool {
        self.as_slice() == TAG_KEY_FIELD
    }
}

// Note that is_field can *NEVER* return true for a `String` because 0xff
// is not a valid UTF-8 character, and thus can not be a valid Rust
// String.

// converts a Node from the RPC layer into a datafusion logical expr
fn convert_node_to_expr(node: RPCNode) -> Result<Expr> {
    let RPCNode {
        children,
        node_type: _,
        value,
    } = node;
    let inputs = children
        .into_iter()
        .map(convert_node_to_expr)
        .collect::<Result<Vec<_>>>()?;

    let value = value.expect("Normalization removed all None values");
    build_node(value, inputs)
}

fn make_tag_name(tag_name: Vec<u8>) -> Result<String> {
    // These should have been handled at a higher level -- if we get
    // here it is too late
    if tag_name.is_measurement() {
        InternalInvalidMeasurementReference.fail()
    } else if tag_name.is_field() {
        InternalInvalidFieldReference.fail()
    } else {
        String::from_utf8(tag_name).context(ConvertingTagName)
    }
}

// Builds an Expr given the Value and the converted children
fn build_node(value: RPCValue, inputs: Vec<Expr>) -> Result<Expr> {
    // Only logical / comparison ops can have inputs.
    let can_have_children = matches!(&value, RPCValue::Logical(_) | RPCValue::Comparison(_));

    if !can_have_children && !inputs.is_empty() {
        return UnexpectedChildren { value }.fail();
    }

    match value {
        RPCValue::StringValue(s) => Ok(lit(s)),
        RPCValue::BoolValue(b) => Ok(lit(b)),
        RPCValue::IntValue(v) => Ok(lit(v)),
        RPCValue::UintValue(v) => Ok(lit(v)),
        RPCValue::FloatValue(f) => Ok(lit(f)),
        RPCValue::RegexValue(pattern) => Ok(lit(pattern)),
        RPCValue::TagRefValue(tag_name) => Ok(col(&make_tag_name(tag_name)?)),
        RPCValue::FieldRefValue(field_name) => Ok(col(&field_name)),
        RPCValue::Logical(logical) => build_logical_node(logical, inputs),
        RPCValue::Comparison(comparison) => build_comparison_node(comparison, inputs),
    }
}

/// Creates an expr from a "Logical" Node
fn build_logical_node(logical: i32, inputs: Vec<Expr>) -> Result<Expr> {
    let logical_enum = RPCLogical::from_i32(logical);

    match logical_enum {
        Some(RPCLogical::And) => build_binary_expr(Operator::And, inputs),
        Some(RPCLogical::Or) => build_binary_expr(Operator::Or, inputs),
        None => UnknownLogicalNode { logical }.fail(),
    }
}

/// Creates an expr from a "Comparison" Node
fn build_comparison_node(comparison: i32, inputs: Vec<Expr>) -> Result<Expr> {
    let comparison_enum = RPCComparison::from_i32(comparison);

    match comparison_enum {
        Some(RPCComparison::Equal) => build_binary_expr(Operator::Eq, inputs),
        Some(RPCComparison::NotEqual) => build_binary_expr(Operator::NotEq, inputs),
        Some(RPCComparison::StartsWith) => StartsWithNotSupported {}.fail(),
        Some(RPCComparison::Regex) => build_regex_match_expr(true, inputs),
        Some(RPCComparison::NotRegex) => build_regex_match_expr(false, inputs),
        Some(RPCComparison::Lt) => build_binary_expr(Operator::Lt, inputs),
        Some(RPCComparison::Lte) => build_binary_expr(Operator::LtEq, inputs),
        Some(RPCComparison::Gt) => build_binary_expr(Operator::Gt, inputs),
        Some(RPCComparison::Gte) => build_binary_expr(Operator::GtEq, inputs),
        None => UnknownComparisonNode { comparison }.fail(),
    }
}

/// Creates a datafusion binary expression with the specified operator
fn build_binary_expr(op: Operator, inputs: Vec<Expr>) -> Result<Expr> {
    // convert input vector to options so we can "take" elements out of it
    let mut inputs = inputs.into_iter().map(Some).collect::<Vec<_>>();

    let num_children = inputs.len();
    match num_children {
        2 => Ok(binary_expr(
            inputs[0].take().unwrap(),
            op,
            inputs[1].take().unwrap(),
        )),
        _ => UnsupportedNumberOfChildren { op, num_children }.fail(),
    }
}

// Creates a DataFusion ScalarUDF expression that performs a regex matching
// operation.
fn build_regex_match_expr(matches: bool, mut inputs: Vec<Expr>) -> Result<Expr> {
    let num_children = inputs.len();
    match num_children {
        2 => {
            let pattern = if let Expr::Literal(ScalarValue::Utf8(pattern)) = inputs.remove(1) {
                pattern.context(RegExpPatternInvalid)?
            } else {
                return InternalInvalidRegexExprReference.fail();
            };

            Ok(regex::regex_match_expr(inputs.remove(0), pattern, matches))
        }
        _ => InternalInvalidRegexExprChildren { num_children }.fail(),
    }
}

pub fn make_read_group_aggregate(
    aggregate: Option<RPCAggregate>,
    group: RPCGroup,
    group_keys: Vec<String>,
) -> Result<GroupByAndAggregate> {
    // validate Group setting
    match group {
        // Group:None is invalid if grouping keys are specified
        RPCGroup::None if !group_keys.is_empty() => InvalidGroupNone {
            num_group_keys: group_keys.len(),
        }
        .fail(),
        _ => Ok(()),
    }?;

    let gby_agg = GroupByAndAggregate::Columns {
        agg: convert_aggregate(aggregate)?,
        group_columns: group_keys,
    };
    Ok(gby_agg)
}

/// Builds GroupByAndAggregate::Windows
pub fn make_read_window_aggregate(
    aggregates: Vec<RPCAggregate>,
    window_every: i64,
    offset: i64,
    window: Option<RPCWindow>,
) -> Result<GroupByAndAggregate> {
    // only support single aggregate for now
    if aggregates.len() != 1 {
        return AggregateNotSingleton { aggregates }.fail();
    }
    let agg = convert_aggregate(aggregates.into_iter().next())?;

    // Translation from these parameters to window bound
    // is defined in the Go code:
    // https://github.com/influxdata/idpe/pull/8636/files#diff-94c0a8d7e427e2d7abe49f01dced50ad776b65ec8f2c8fb2a2c8b90e2e377ed5R82
    //
    // Quoting:
    //
    // Window and the WindowEvery/Offset should be mutually
    // exclusive. If you set either the WindowEvery or Offset with
    // nanosecond values, then the Window will be ignored

    let (every, offset) = match (window, window_every, offset) {
        (None, 0, 0) => return EmptyWindow {}.fail(),
        (Some(window), 0, 0) => (
            convert_duration(window.every, DurationValidation::ForbidZero).map_err(|e| {
                Error::InvalidWindowEveryDuration {
                    description: e.into(),
                }
            })?,
            convert_duration(window.offset, DurationValidation::AllowZero).map_err(|e| {
                Error::InvalidWindowOffsetDuration {
                    description: e.into(),
                }
            })?,
        ),
        (window, window_every, offset) => {
            // warn if window is being ignored
            if window.is_some() {
                warn!("window_every {} or offset {} was non zero, so ignoring window specification '{:?}' on read_window_aggregate",
                      window_every, offset, window);
            }
            (
                WindowDuration::from_nanoseconds(window_every),
                WindowDuration::from_nanoseconds(offset),
            )
        }
    };

    Ok(GroupByAndAggregate::Window { agg, every, offset })
}

enum DurationValidation {
    /// Zero windows are allowed
    AllowZero,
    /// Zero windows are not allowed
    ForbidZero,
}

/// Convert the RPC input to an IOx WindowDuration
/// structure. `zero_validation` specifies what to do if the window is empty
fn convert_duration(
    duration: Option<RPCDuration>,
    zero_validation: DurationValidation,
) -> Result<WindowDuration, &'static str> {
    let duration = duration.ok_or("No duration specified in RPC")?;

    match (duration.nsecs, duration.months, zero_validation) {
        // Same error as Go code: https://github.com/influxdata/flux/blob/master/execute/window.go#L36
        (0, 0, DurationValidation::ForbidZero) => {
            Err("duration used as an interval cannot be zero")
        }
        (0, 0, DurationValidation::AllowZero) => Ok(WindowDuration::empty()),
        (nsecs, 0, _) => Ok(WindowDuration::from_nanoseconds(nsecs)),
        (0, _, _) => Ok(WindowDuration::from_months(
            duration.months,
            duration.negative,
        )),
        (_, _, _) => Err("duration used as an interval cannot mix month and nanosecond units"),
    }
}

fn convert_aggregate(aggregate: Option<RPCAggregate>) -> Result<QueryAggregate> {
    let aggregate = match aggregate {
        None => return Ok(QueryAggregate::None),
        Some(aggregate) => aggregate,
    };

    let aggregate_type = aggregate.r#type;
    let aggregate_type_enum = RPCAggregateType::from_i32(aggregate_type);

    match aggregate_type_enum {
        Some(RPCAggregateType::None) => Ok(QueryAggregate::None),
        Some(RPCAggregateType::Sum) => Ok(QueryAggregate::Sum),
        Some(RPCAggregateType::Count) => Ok(QueryAggregate::Count),
        Some(RPCAggregateType::Min) => Ok(QueryAggregate::Min),
        Some(RPCAggregateType::Max) => Ok(QueryAggregate::Max),
        Some(RPCAggregateType::First) => Ok(QueryAggregate::First),
        Some(RPCAggregateType::Last) => Ok(QueryAggregate::Last),
        Some(RPCAggregateType::Mean) => Ok(QueryAggregate::Mean),
        None => UnknownAggregate { aggregate_type }.fail(),
    }
}

pub fn convert_group_type(group: i32) -> Result<RPCGroup> {
    RPCGroup::from_i32(group).ok_or(Error::UnknownGroup { group_type: group })
}

/// Creates a representation of some struct (in another crate that we
/// don't control) suitable for logging with `std::fmt::Display`)
pub trait Loggable<'a> {
    fn loggable(&'a self) -> Box<dyn fmt::Display + 'a>;
}

impl<'a> Loggable<'a> for Option<RPCPredicate> {
    fn loggable(&'a self) -> Box<dyn fmt::Display + 'a> {
        Box::new(displayable_predicate(self.as_ref()))
    }
}

impl<'a> Loggable<'a> for RPCPredicate {
    fn loggable(&'a self) -> Box<dyn fmt::Display + 'a> {
        Box::new(displayable_predicate(Some(self)))
    }
}

/// Returns a struct that can format gRPC predicate (aka `RPCPredicates`) for
/// Display
///
/// For example:
/// let pred = RPCPredicate (...);
/// println!("The predicate is {:?}", loggable_predicate(pred));
pub fn displayable_predicate(pred: Option<&RPCPredicate>) -> impl fmt::Display + '_ {
    struct Wrapper<'a>(Option<&'a RPCPredicate>);

    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0 {
                None => write!(f, "<NONE>"),
                Some(pred) => format_predicate(pred, f),
            }
        }
    }
    Wrapper(pred)
}

fn format_predicate<'a>(pred: &'a RPCPredicate, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match &pred.root {
        Some(r) => format_node(r, f),
        None => write!(f, "root: <NONE>"),
    }
}

fn format_node<'a>(node: &'a RPCNode, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let value = match &node.value {
        None => {
            write!(f, "node: NONE")?;
            return Ok(());
        }
        Some(value) => value,
    };

    match node.children.len() {
        0 => {
            format_value(value, f)?;
        }
        // print using infix notation
        // (child0 <op> child1)
        2 => {
            write!(f, "(")?;
            format_node(&node.children[0], f)?;
            write!(f, " ")?;
            format_value(value, f)?;
            write!(f, " ")?;
            format_node(&node.children[1], f)?;
            write!(f, ")")?;
        }
        // print func notation
        // <op>(child0, chold1, ...)
        _ => {
            format_value(value, f)?;
            write!(f, "(")?;
            for (i, child) in node.children.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                format_node(child, f)?;
            }
            write!(f, ")")?;
        }
    };

    Ok(())
}

fn format_value<'a>(value: &'a RPCValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    use RPCValue::*;
    match value {
        StringValue(s) => write!(f, "\"{}\"", s),
        BoolValue(b) => write!(f, "{}", b),
        IntValue(i) => write!(f, "{}", i),
        UintValue(u) => write!(f, "{}", u),
        FloatValue(fval) => write!(f, "{}", fval),
        RegexValue(r) => write!(f, "RegEx:{}", r),
        TagRefValue(bytes) => {
            let temp = String::from_utf8_lossy(bytes);
            let sval = match bytes.as_slice() {
                TAG_KEY_MEASUREMENT => "_m[0x00]",
                TAG_KEY_FIELD => "_f[0xff]",
                _ => &temp,
            };
            write!(f, "TagRef:{}", sval)
        }
        FieldRefValue(d) => write!(f, "FieldRef:{}", d),
        Logical(v) => format_logical(*v, f),
        Comparison(v) => format_comparison(*v, f),
    }
}

fn format_logical(v: i32, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match RPCLogical::from_i32(v) {
        Some(RPCLogical::And) => write!(f, "AND"),
        Some(RPCLogical::Or) => write!(f, "Or"),
        None => write!(f, "UNKNOWN_LOGICAL:{}", v),
    }
}

fn format_comparison(v: i32, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match RPCComparison::from_i32(v) {
        Some(RPCComparison::Equal) => write!(f, "=="),
        Some(RPCComparison::NotEqual) => write!(f, "!="),
        Some(RPCComparison::StartsWith) => write!(f, "StartsWith"),
        Some(RPCComparison::Regex) => write!(f, "RegEx"),
        Some(RPCComparison::NotRegex) => write!(f, "NotRegex"),
        Some(RPCComparison::Lt) => write!(f, "<"),
        Some(RPCComparison::Lte) => write!(f, "<="),
        Some(RPCComparison::Gt) => write!(f, ">"),
        Some(RPCComparison::Gte) => write!(f, ">="),
        None => write!(f, "UNKNOWN_COMPARISON:{}", v),
    }
}

#[cfg(test)]
mod tests {
    use generated_types::node::Type as RPCNodeType;
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_convert_predicate_none() {
        let predicate = PredicateBuilder::default()
            .rpc_predicate(None)
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
    }

    #[test]
    fn test_convert_predicate_empty() {
        let rpc_predicate = RPCPredicate { root: None };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Unexpected empty predicate: Node";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_good() {
        let (comparison, expected_expr) = make_host_comparison();

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .expect("successfully converting predicate")
            .build();

        assert_eq!(predicate.exprs.len(), 1);
        let converted_expr = &predicate.exprs[0];

        // compare the expression using their string representations
        // as Expr can't be compared directly.
        let converted_expr = format!("{:?}", converted_expr);
        let expected_expr = format!("{:?}", expected_expr);

        assert_eq!(
            expected_expr, converted_expr,
            "expected '{:#?}' doesn't match actual '{:#?}'",
            expected_expr, converted_expr
        );
    }

    #[test]
    fn test_convert_predicate_no_children() {
        let comparison = RPCNode {
            node_type: RPCNodeType::ComparisonExpression as i32,
            children: vec![],
            value: Some(RPCValue::Comparison(RPCComparison::Gt as i32)),
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unsupported number of children in binary operator Gt: 0 (must be 2)";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_comparison_bad_values() {
        // Send in invalid input to simulate a bad actor
        let iconst = RPCNode {
            node_type: RPCNodeType::Literal as i32,
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };

        let comparison = RPCNode {
            node_type: RPCNodeType::ComparisonExpression as i32,
            children: vec![iconst.clone(), iconst],
            value: Some(RPCValue::Comparison(42)), // 42 is not a valid comparison value
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unknown comparison node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_logical_bad_values() {
        // Send in invalid input to simulate a bad actor
        let iconst = RPCNode {
            node_type: RPCNodeType::Literal as i32,
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };

        let comparison = RPCNode {
            node_type: RPCNodeType::LogicalExpression as i32,
            children: vec![iconst.clone(), iconst],
            value: Some(RPCValue::Logical(42)), // 42 is not a valid logical value
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unknown logical node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_field_selection() {
        let field_selection = make_field_ref_node("field1");

        let rpc_predicate = RPCPredicate {
            root: Some(field_selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(predicate.field_columns, Some(to_set(&["field1"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_field_selection_wrapped() {
        // test wrapping the whole predicate in a None value (aka what influxql does for
        // some reason
        let field_selection = make_field_ref_node("field1");
        let wrapped = RPCNode {
            node_type: RPCNodeType::ParenExpression as i32,
            children: vec![field_selection],
            value: None,
        };

        let rpc_predicate = RPCPredicate {
            root: Some(wrapped),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(predicate.field_columns, Some(to_set(&["field1"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_multiple_field_selection() {
        let selection = make_or_node(make_field_ref_node("field1"), make_field_ref_node("field2"));
        let selection = make_or_node(selection, make_field_ref_node("field3"));

        let rpc_predicate = RPCPredicate {
            root: Some(selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(
            predicate.field_columns,
            Some(to_set(&["field1", "field2", "field3"]))
        );
        assert!(predicate.range.is_none());
    }

    // test multiple field restrictions and a general predicate
    #[test]
    fn test_convert_predicate_multiple_field_selection_and_predicate() {
        let (comparison, expected_expr) = make_host_comparison();

        let selection = make_or_node(make_field_ref_node("field1"), make_field_ref_node("field2"));

        let selection = make_and_node(selection, comparison);

        let rpc_predicate = RPCPredicate {
            root: Some(selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        // compare the expression using their string representations
        // as Expr can't be compared directly.
        assert_eq!(predicate.exprs.len(), 1);
        let converted_expr = format!("{:?}", predicate.exprs[0]);
        let expected_expr = format!("{:?}", expected_expr);

        assert_eq!(
            expected_expr, converted_expr,
            "expected '{:#?}' doesn't match actual '{:#?}'",
            expected_expr, converted_expr
        );
        assert!(predicate.table_names.is_none());

        assert_eq!(predicate.field_columns, Some(to_set(&["field1", "field2"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_measurement_selection() {
        let measurement_selection = make_measurement_ref_node("m1");

        let rpc_predicate = RPCPredicate {
            root: Some(measurement_selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert_eq!(predicate.table_names, Some(to_set(&["m1"])));
        assert!(predicate.field_columns.is_none());
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_unsupported_structure() {
        // Test (_f = "foo" and host > 5.0) OR (_m = "bar")
        // which is not something we know how to do

        let (comparison, _) = make_host_comparison();

        let unsupported = make_or_node(
            make_and_node(make_field_ref_node("foo"), comparison),
            make_measurement_ref_node("bar"),
        );

        let rpc_predicate = RPCPredicate {
            root: Some(unsupported),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Internal error: found field tag reference in unexpected location";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    /// make a _f = 'field_name' type node
    fn make_field_ref_node(field_name: impl Into<String>) -> RPCNode {
        make_tag_ref_node(&[255], field_name)
    }

    /// make a _m = 'measurement_name' type node
    fn make_measurement_ref_node(field_name: impl Into<String>) -> RPCNode {
        make_tag_ref_node(&[0], field_name)
    }

    /// returns (RPCNode, and expected_expr for the "host > 5.0")
    fn make_host_comparison() -> (RPCNode, Expr) {
        // host > 5.0
        let field_ref = RPCNode {
            node_type: RPCNodeType::FieldRef as i32,
            children: vec![],
            value: Some(RPCValue::FieldRefValue(String::from("host"))),
        };
        let iconst = RPCNode {
            node_type: RPCNodeType::Literal as i32,
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };
        let comparison = RPCNode {
            node_type: RPCNodeType::ComparisonExpression as i32,
            children: vec![field_ref, iconst],
            value: Some(RPCValue::Comparison(RPCComparison::Gt as i32)),
        };

        let expected_expr = col("host").gt(lit(5.0));

        (comparison, expected_expr)
    }

    fn make_tag_ref_node(tag_name: &[u8], field_name: impl Into<String>) -> RPCNode {
        let field_tag_ref_node = RPCNode {
            node_type: RPCNodeType::TagRef as i32,
            children: vec![],
            value: Some(RPCValue::TagRefValue(tag_name.to_vec())),
        };

        let string_node = RPCNode {
            node_type: RPCNodeType::Literal as i32,
            children: vec![],
            value: Some(RPCValue::StringValue(field_name.into())),
        };

        RPCNode {
            node_type: RPCNodeType::ComparisonExpression as i32,
            children: vec![field_tag_ref_node, string_node],
            value: Some(RPCValue::Comparison(RPCComparison::Equal as i32)),
        }
    }

    /// make n1 OR n2
    fn make_or_node(n1: RPCNode, n2: RPCNode) -> RPCNode {
        RPCNode {
            node_type: RPCNodeType::LogicalExpression as i32,
            children: vec![n1, n2],
            value: Some(RPCValue::Logical(RPCLogical::Or as i32)),
        }
    }

    /// make n1 AND n2
    fn make_and_node(n1: RPCNode, n2: RPCNode) -> RPCNode {
        RPCNode {
            node_type: RPCNodeType::LogicalExpression as i32,
            children: vec![n1, n2],
            value: Some(RPCValue::Logical(RPCLogical::And as i32)),
        }
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
    }

    /// Return the dislay formay of the resulting error, or
    /// 'UNEXPECTED SUCCESS' if `res` is not an error.
    fn error_result_to_string<R>(res: Result<R>) -> String {
        match res {
            Ok(_) => "UNEXPECTED SUCCESS".into(),
            Err(e) => format!("{}", e),
        }
    }

    #[test]
    fn test_make_read_group_aggregate() {
        assert_eq!(
            make_read_group_aggregate(Some(make_aggregate(1)), RPCGroup::None, vec![]).unwrap(),
            GroupByAndAggregate::Columns {
                agg: QueryAggregate::Sum,
                group_columns: vec![]
            }
        );

        assert_eq!(
            make_read_group_aggregate(Some(make_aggregate(1)), RPCGroup::By, vec!["gcol".into()])
                .unwrap(),
            GroupByAndAggregate::Columns {
                agg: QueryAggregate::Sum,
                group_columns: vec!["gcol".into()]
            }
        );

        // Flux does send this kind of request
        assert_eq!(
            make_read_group_aggregate(None, RPCGroup::By, vec![]).unwrap(),
            GroupByAndAggregate::Columns {
                agg: QueryAggregate::None,
                group_columns: vec![]
            }
        );

        assert_eq!(
            make_read_group_aggregate(Some(make_aggregate(1)), RPCGroup::None, vec!["gcol".into()])
                .unwrap_err()
                .to_string(),
            "Incompatible read_group request: Group::None had 1 group keys (expected 0)"
        );

        assert_eq!(
            make_read_group_aggregate(Some(make_aggregate(1)), RPCGroup::By, vec![]).unwrap(),
            GroupByAndAggregate::Columns {
                agg: QueryAggregate::Sum,
                group_columns: vec![]
            }
        );
    }

    #[test]
    fn test_make_read_window_aggregate() {
        let pos_5_ns = WindowDuration::from_nanoseconds(5);
        let pos_10_ns = WindowDuration::from_nanoseconds(10);
        let pos_3_months = WindowDuration::from_months(3, false);
        let neg_1_months = WindowDuration::from_months(1, true);

        let agg = make_read_window_aggregate(vec![], 5, 10, None);
        let expected =
            "Error creating aggregate: Exactly one aggregate is supported, but 0 were supplied: []";
        assert_eq!(error_result_to_string(agg), expected);

        let agg =
            make_read_window_aggregate(vec![make_aggregate(1), make_aggregate(2)], 5, 10, None);
        let expected = "Error creating aggregate: Exactly one aggregate is supported, but 2 were supplied: [Aggregate { r#type: Sum }, Aggregate { r#type: Count }]";
        assert_eq!(error_result_to_string(agg), expected);

        // now window specified
        let agg = make_read_window_aggregate(vec![make_aggregate(1)], 0, 0, None);
        let expected = "Error parsing window bounds: No window specified";
        assert_eq!(error_result_to_string(agg), expected);

        // correct window + window_every
        let agg = make_read_window_aggregate(vec![make_aggregate(1)], 5, 10, None).unwrap();
        let expected = make_storage_window(QueryAggregate::Sum, &pos_5_ns, &pos_10_ns);
        assert_eq!(agg, expected);

        // correct every + offset
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(5, 0, false, 10, 0, false)),
        )
        .unwrap();
        let expected = make_storage_window(QueryAggregate::Sum, &pos_5_ns, &pos_10_ns);
        assert_eq!(agg, expected);

        // correct every + zero offset
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(5, 0, false, 0, 0, false)),
        )
        .unwrap();
        let expected =
            make_storage_window(QueryAggregate::Sum, &pos_5_ns, &WindowDuration::empty());
        assert_eq!(agg, expected);

        // correct every + offset in months
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(0, 3, false, 0, 1, true)),
        )
        .unwrap();
        let expected = make_storage_window(QueryAggregate::Sum, &pos_3_months, &neg_1_months);
        assert_eq!(agg, expected);

        // correct every + offset in months
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(0, 1, true, 0, 3, false)),
        )
        .unwrap();
        let expected = make_storage_window(QueryAggregate::Sum, &neg_1_months, &pos_3_months);
        assert_eq!(agg, expected);

        // both window + window_every and every + offset -- every + offset overrides
        // (100 and 200 should be ignored)
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            5,
            10,
            Some(make_rpc_window(100, 0, false, 200, 0, false)),
        )
        .unwrap();
        let expected = make_storage_window(QueryAggregate::Sum, &pos_5_ns, &pos_10_ns);
        assert_eq!(agg, expected);

        // invalid durations
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(5, 1, false, 10, 0, false)),
        );
        let expected = "Error parsing window bounds duration \'window.every\': duration used as an interval cannot mix month and nanosecond units";
        assert_eq!(error_result_to_string(agg), expected);

        // invalid durations
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(5, 0, false, 10, 1, false)),
        );
        let expected = "Error parsing window bounds duration \'window.offset\': duration used as an interval cannot mix month and nanosecond units";
        assert_eq!(error_result_to_string(agg), expected);

        // invalid durations
        let agg = make_read_window_aggregate(
            vec![make_aggregate(1)],
            0,
            0,
            Some(make_rpc_window(0, 0, false, 5, 0, false)),
        );
        let expected = "Error parsing window bounds duration \'window.every\': duration used as an interval cannot be zero";
        assert_eq!(error_result_to_string(agg), expected);
    }

    #[test]
    fn test_convert_group_type() {
        assert_eq!(convert_group_type(0).unwrap(), RPCGroup::None);
        assert_eq!(convert_group_type(2).unwrap(), RPCGroup::By);
        assert_eq!(
            error_result_to_string(convert_group_type(1)),
            "Error creating aggregate: Unknown group type: 1"
        );
    }

    #[test]
    fn test_convert_aggregate() {
        assert_eq!(convert_aggregate(None).unwrap(), QueryAggregate::None);
        assert_eq!(
            convert_aggregate(Some(make_aggregate(0))).unwrap(),
            QueryAggregate::None
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(1))).unwrap(),
            QueryAggregate::Sum
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(2))).unwrap(),
            QueryAggregate::Count
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(3))).unwrap(),
            QueryAggregate::Min
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(4))).unwrap(),
            QueryAggregate::Max
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(5))).unwrap(),
            QueryAggregate::First
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(6))).unwrap(),
            QueryAggregate::Last
        );
        assert_eq!(
            convert_aggregate(Some(make_aggregate(7))).unwrap(),
            QueryAggregate::Mean
        );
        assert_eq!(
            error_result_to_string(convert_aggregate(Some(make_aggregate(100)))),
            "Error creating aggregate: Unknown aggregate type 100"
        );
    }

    fn make_aggregate(t: i32) -> RPCAggregate {
        RPCAggregate { r#type: t }
    }

    fn make_rpc_window(
        every_nsecs: i64,
        every_months: i64,
        every_negative: bool,
        offset_nsecs: i64,
        offset_months: i64,
        offset_negative: bool,
    ) -> RPCWindow {
        RPCWindow {
            every: Some(RPCDuration {
                nsecs: every_nsecs,
                months: every_months,
                negative: every_negative,
            }),
            offset: Some(RPCDuration {
                nsecs: offset_nsecs,
                months: offset_months,
                negative: offset_negative,
            }),
        }
    }

    fn make_storage_window(
        agg: QueryAggregate,
        every: &WindowDuration,
        offset: &WindowDuration,
    ) -> GroupByAndAggregate {
        GroupByAndAggregate::Window {
            agg,
            every: every.clone(),
            offset: offset.clone(),
        }
    }

    #[test]
    fn test_displayable_predicate_none() {
        let rpc_pred = None;

        assert_eq!(
            "<NONE>",
            format!("{}", displayable_predicate(rpc_pred.as_ref()))
        );
    }

    #[test]
    fn test_displayable_predicate_root_none() {
        let rpc_pred = Some(RPCPredicate { root: None });

        assert_eq!(
            "root: <NONE>",
            format!("{}", displayable_predicate(rpc_pred.as_ref()))
        );
    }

    #[test]
    fn test_displayable_predicate_two_args() {
        let (comparison, _) = make_host_comparison();
        let rpc_pred = Some(RPCPredicate {
            root: Some(comparison),
        });
        assert_eq!(
            "(FieldRef:host > 5)",
            format!("{}", displayable_predicate(rpc_pred.as_ref()))
        );
    }

    #[test]
    fn test_displayable_predicate_three_args() {
        // Make one with more than two children (not sure if this ever happens)
        let node = RPCNode {
            node_type: RPCNodeType::LogicalExpression as i32,
            children: vec![
                make_tag_ref_node(b"tag1", "val1"),
                make_tag_ref_node(b"tag2", "val2"),
                make_tag_ref_node(b"tag3", "val3"),
            ],
            value: Some(RPCValue::Logical(RPCLogical::And as i32)),
        };
        let rpc_pred = Some(RPCPredicate { root: Some(node) });
        assert_eq!(
            "AND((TagRef:tag1 == \"val1\"), (TagRef:tag2 == \"val2\"), (TagRef:tag3 == \"val3\"))",
            format!("{}", displayable_predicate(rpc_pred.as_ref()))
        );
    }

    #[test]
    fn test_displayable_predicate_mesurement_and_field() {
        // Make one with more than two children (not sure if this ever happens)
        let node = RPCNode {
            node_type: RPCNodeType::LogicalExpression as i32,
            children: vec![
                make_tag_ref_node(&[0], "val1"),
                make_tag_ref_node(b"tag2", "val2"),
                make_tag_ref_node(&[255], "val3"),
            ],
            value: Some(RPCValue::Logical(RPCLogical::And as i32)),
        };
        let rpc_pred = Some(RPCPredicate { root: Some(node) });
        assert_eq!(
            "AND((TagRef:_m[0x00] == \"val1\"), (TagRef:tag2 == \"val2\"), (TagRef:_f[0xff] == \"val3\"))",
            format!("{}", displayable_predicate(rpc_pred.as_ref()))
        );
    }
}
