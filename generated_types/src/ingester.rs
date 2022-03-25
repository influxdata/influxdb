use crate::{
    google::{FieldViolation, FieldViolationExt, OptionalField},
    influxdata::iox::ingester::v1 as proto,
};
use data_types::timestamp::TimestampRange;
use data_types2::{IngesterQueryRequest, SequencerId};
use datafusion::{
    logical_plan::{
        abs, acos, asin, atan, ceil, concat, cos, digest, exp, floor, ln, log10, log2, round,
        signum, sin, sqrt, tan, trunc,
        window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
        Column, Expr, Operator,
    },
    physical_plan::{aggregates, functions, window_functions},
    prelude::{
        array, date_part, date_trunc, lower, ltrim, md5, octet_length, rtrim, sha224, sha256,
        sha384, sha512, trim, upper,
    },
};
use predicate::{BinaryExpr, Predicate};

impl TryFrom<proto::IngesterQueryRequest> for IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(proto: proto::IngesterQueryRequest) -> Result<Self, Self::Error> {
        let proto::IngesterQueryRequest {
            namespace,
            sequencer_id,
            table,
            columns,
            predicate,
        } = proto;

        let predicate = predicate.map(TryInto::try_into).transpose()?;
        let sequencer_id: i16 = sequencer_id.try_into().scope("sequencer_id")?;

        Ok(Self::new(
            namespace,
            SequencerId::new(sequencer_id),
            table,
            columns,
            predicate,
        ))
    }
}

impl TryFrom<IngesterQueryRequest> for proto::IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(query: IngesterQueryRequest) -> Result<Self, Self::Error> {
        let IngesterQueryRequest {
            namespace,
            sequencer_id,
            table,
            columns,
            predicate,
        } = query;

        Ok(Self {
            namespace,
            sequencer_id: sequencer_id.get().into(),
            table,
            columns,
            predicate: predicate.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<Predicate> for proto::Predicate {
    type Error = FieldViolation;

    fn try_from(pred: Predicate) -> Result<Self, Self::Error> {
        let Predicate {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        } = pred;

        let field_columns = field_columns.into_iter().flatten().collect();
        let range = range.map(|r| proto::TimestampRange {
            start: r.start(),
            end: r.end(),
        });
        let exprs = exprs
            .into_iter()
            .map(from_expr)
            .collect::<Result<Vec<_>, _>>()?;
        let value_expr = value_expr
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = FieldViolation;

    fn try_from(proto: proto::Predicate) -> Result<Self, Self::Error> {
        let proto::Predicate {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        } = proto;

        let field_columns = if field_columns.is_empty() {
            None
        } else {
            Some(field_columns.into_iter().collect())
        };

        let range = range.map(|r| TimestampRange::new(r.start, r.end));

        let exprs = exprs
            .into_iter()
            .map(from_proto_expr)
            .collect::<Result<Vec<_>, _>>()?;

        let value_expr = value_expr
            .into_iter()
            .map(|ve| {
                let left = ve.left.unwrap_field("left")?;
                let right = ve.right.unwrap_field("right")?;

                Ok(BinaryExpr {
                    left: from_proto_column(left),
                    op: from_proto_binary_op("op", &ve.op)?,
                    right: from_proto_expr(right)?,
                })
            })
            .collect::<Result<Vec<BinaryExpr>, FieldViolation>>()?;

        Ok(Self {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<BinaryExpr> for proto::BinaryExpr {
    type Error = FieldViolation;

    fn try_from(bin_expr: BinaryExpr) -> Result<Self, Self::Error> {
        let BinaryExpr { left, op, right } = bin_expr;

        Ok(Self {
            left: Some(from_column(left)),
            op: op.to_string(),
            right: Some(from_expr(right)?),
        })
    }
}

fn from_column(column: Column) -> proto::Column {
    proto::Column {
        relation: column
            .relation
            .map(|relation| proto::ColumnRelation { relation }),
        name: column.name,
    }
}

fn from_proto_column(proto: proto::Column) -> Column {
    Column {
        relation: proto.relation.map(|r| r.relation),
        name: proto.name,
    }
}

fn from_proto_binary_op(field: &'static str, op: &str) -> Result<Operator, FieldViolation> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
        other => Err(proto_error(
            field,
            format!("Unsupported binary operator '{:?}'", other),
        )),
    }
}

fn boxed_expr(
    expr: Option<Box<proto::LogicalExprNode>>,
    field_name: &'static str,
) -> Result<Box<Expr>, FieldViolation> {
    Ok(Box::new(from_proto_expr(*expr.unwrap_field(field_name)?)?))
}

fn from_proto_expr(proto: proto::LogicalExprNode) -> Result<Expr, FieldViolation> {
    use proto::logical_expr_node::ExprType;

    let expr_type = proto.expr_type.unwrap_field("expr_type")?;
    match expr_type {
        ExprType::BinaryExpr(binary_expr) => Ok(Expr::BinaryExpr {
            left: boxed_expr(binary_expr.l, "l")?,
            op: from_proto_binary_op("op", &binary_expr.op)?,
            right: boxed_expr(binary_expr.r, "r")?,
        }),
        ExprType::Column(column) => Ok(Expr::Column(from_proto_column(column))),
        ExprType::Literal(literal) => Ok(Expr::Literal(from_proto_scalar_value(literal)?)),
        ExprType::WindowExpr(expr) => {
            let window_function = expr.window_function.unwrap_field("window_function")?;
            let partition_by = expr
                .partition_by
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = expr
                .order_by
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let window_frame = expr
                .window_frame
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    if proto::WindowFrameUnits::Range == window_frame.window_frame_units()
                        && order_by.len() != 1
                    {
                        Err(proto_error("order_by", "With window frame of type RANGE, the order by expression must be of length 1"))
                    } else {
                        from_proto_window_frame(window_frame)
                    }
                })
                .transpose()?;

            match window_function {
                proto::window_expr_node::WindowFunction::AggrFunction(i) => {
                    let aggr_function = proto::AggregateFunction::from_i32(i).ok_or_else(|| {
                        proto_error(
                            "window_function",
                            format!("Received an unknown aggregate window function: {}", i),
                        )
                    })?;

                    Ok(Expr::WindowFunction {
                        fun: window_functions::WindowFunction::AggregateFunction(
                            from_proto_aggr_function(aggr_function)?,
                        ),
                        args: vec![from_proto_expr(*expr.expr.unwrap_field("expr")?)?],
                        partition_by,
                        order_by,
                        window_frame,
                    })
                }
                proto::window_expr_node::WindowFunction::BuiltInFunction(i) => {
                    use proto::BuiltInWindowFunction::*;
                    use window_functions::BuiltInWindowFunction;

                    let built_in = proto::BuiltInWindowFunction::from_i32(i)
                        .unwrap_field("built_in_window_function")?;
                    let built_in_function = match built_in {
                        Unspecified => {
                            return Err(proto_error("built_in_window_function", "not specified"))
                        }
                        RowNumber => BuiltInWindowFunction::RowNumber,
                        Rank => BuiltInWindowFunction::Rank,
                        PercentRank => BuiltInWindowFunction::PercentRank,
                        DenseRank => BuiltInWindowFunction::DenseRank,
                        Lag => BuiltInWindowFunction::Lag,
                        Lead => BuiltInWindowFunction::Lead,
                        FirstValue => BuiltInWindowFunction::FirstValue,
                        CumeDist => BuiltInWindowFunction::CumeDist,
                        Ntile => BuiltInWindowFunction::Ntile,
                        NthValue => BuiltInWindowFunction::NthValue,
                        LastValue => BuiltInWindowFunction::LastValue,
                    };

                    Ok(Expr::WindowFunction {
                        fun: window_functions::WindowFunction::BuiltInWindowFunction(
                            built_in_function,
                        ),
                        args: vec![from_proto_expr(*expr.expr.unwrap_field("expr")?)?],
                        partition_by,
                        order_by,
                        window_frame,
                    })
                }
            }
        }
        ExprType::AggregateExpr(expr) => {
            use aggregates::AggregateFunction;
            use proto::AggregateFunction::*;

            let aggr_function = proto::AggregateFunction::from_i32(expr.aggr_function)
                .unwrap_field("aggr_function")?;
            let fun = match aggr_function {
                Unspecified => return Err(proto_error("aggr_function", "not specified")),
                Min => AggregateFunction::Min,
                Max => AggregateFunction::Max,
                Sum => AggregateFunction::Sum,
                Avg => AggregateFunction::Avg,
                Count => AggregateFunction::Count,
                ApproxDistinct => AggregateFunction::ApproxDistinct,
                ArrayAgg => AggregateFunction::ArrayAgg,
                Variance => AggregateFunction::Variance,
                VariancePop => AggregateFunction::VariancePop,
                Covariance => AggregateFunction::Covariance,
                CovariancePop => AggregateFunction::CovariancePop,
                Stddev => AggregateFunction::Stddev,
                StddevPop => AggregateFunction::StddevPop,
                Correlation => AggregateFunction::Correlation,
                ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
                ApproxMedian => AggregateFunction::ApproxMedian,
            };

            Ok(Expr::AggregateFunction {
                fun,
                args: expr
                    .expr
                    .into_iter()
                    .map(from_proto_expr)
                    .collect::<Result<Vec<_>, _>>()?,
                distinct: false, //TODO
            })
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(
            boxed_expr(alias.expr, "expr")?,
            alias.alias.clone(),
        )),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(boxed_expr(is_null.expr, "expr")?)),
        ExprType::IsNotNullExpr(is_not_null) => {
            Ok(Expr::IsNotNull(boxed_expr(is_not_null.expr, "expr")?))
        }
        ExprType::NotExpr(not) => Ok(Expr::Not(boxed_expr(not.expr, "expr")?)),
        ExprType::Between(between) => Ok(Expr::Between {
            expr: boxed_expr(between.expr, "expr")?,
            negated: between.negated,
            low: boxed_expr(between.low, "low")?,
            high: boxed_expr(between.high, "high")?,
        }),
        ExprType::CaseNode(case) => {
            let proto::CaseNode {
                expr,
                when_then_expr,
                else_expr,
            } = *case;
            let when_then_expr = when_then_expr
                .into_iter()
                .map(|e| {
                    let proto::WhenThen {
                        when_expr,
                        then_expr,
                    } = e;
                    Ok((
                        Box::new(from_proto_expr(when_expr.unwrap_field("when_expr")?)?),
                        Box::new(from_proto_expr(then_expr.unwrap_field("then_expr")?)?),
                    ))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, FieldViolation>>()?;
            let expr: Option<Result<_, FieldViolation>> =
                expr.map(|e| Ok(Box::new(from_proto_expr(*e)?)));
            let else_expr: Option<Result<_, FieldViolation>> =
                else_expr.map(|e| Ok(Box::new(from_proto_expr(*e)?)));
            Ok(Expr::Case {
                expr: expr.transpose()?,
                when_then_expr,
                else_expr: else_expr.transpose()?,
            })
        }
        ExprType::Cast(cast) => {
            let expr = boxed_expr(cast.expr, "expr")?;
            let arrow_type = cast.arrow_type.unwrap_field("arrow_type")?;
            let arrow_type_enum = arrow_type.arrow_type_enum.unwrap_field("arrow_type_enum")?;

            let data_type = from_proto_arrow_type(arrow_type_enum)?;
            Ok(Expr::Cast { expr, data_type })
        }
        ExprType::TryCast(cast) => {
            let expr = boxed_expr(cast.expr, "expr")?;
            let arrow_type = cast.arrow_type.unwrap_field("arrow_type")?;
            let arrow_type_enum = arrow_type.arrow_type_enum.unwrap_field("arrow_type_enum")?;

            let data_type = from_proto_arrow_type(arrow_type_enum)?;
            Ok(Expr::TryCast { expr, data_type })
        }
        ExprType::Sort(sort) => Ok(Expr::Sort {
            expr: boxed_expr(sort.expr, "expr")?,
            asc: sort.asc,
            nulls_first: sort.nulls_first,
        }),
        ExprType::Negative(negative) => Ok(Expr::Negative(boxed_expr(negative.expr, "expr")?)),
        ExprType::InList(in_list) => Ok(Expr::InList {
            expr: boxed_expr(in_list.expr, "expr")?,
            list: in_list
                .list
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?,
            negated: in_list.negated,
        }),
        ExprType::Wildcard(_) => Ok(Expr::Wildcard),
        ExprType::ScalarFunction(expr) => {
            let scalar_function = proto::ScalarFunction::from_i32(expr.fun).unwrap_field("fun")?;
            let mut args = expr
                .args
                .into_iter()
                .map(from_proto_expr)
                .rev()
                .collect::<Result<Vec<_>, _>>()?;

            let scalar_function = match scalar_function {
                proto::ScalarFunction::Unspecified => {
                    return Err(proto_error("fun", "not specified"))
                }
                // unary - must have one argument, otherwise error
                proto::ScalarFunction::Sqrt => sqrt(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sin => sin(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Cos => cos(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Tan => tan(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Asin => asin(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Acos => acos(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Atan => atan(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Exp => exp(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log2 => log2(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ln => ln(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log => log10(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log10 => log10(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Floor => floor(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ceil => ceil(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Round => round(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Trunc => trunc(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Abs => abs(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Signum => signum(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Octetlength => octet_length(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Lower => lower(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Upper => upper(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Trim => trim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ltrim => ltrim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Rtrim => rtrim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Md5 => md5(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha224 => sha224(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha256 => sha256(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha384 => sha384(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha512 => sha512(args.pop().unwrap_field("arg")?),

                // binary - error if there aren't two arguments
                proto::ScalarFunction::Datepart => date_part(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),
                proto::ScalarFunction::Datetrunc => date_trunc(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),
                proto::ScalarFunction::Digest => digest(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),

                // pass the vec of arguments through without checking how many
                proto::ScalarFunction::Concat => concat(&args),
                proto::ScalarFunction::Array => array(args),
                proto::ScalarFunction::Totimestamp => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::ToTimestamp,
                    args,
                },
                proto::ScalarFunction::Totimestampmillis => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::ToTimestampMillis,
                    args,
                },
                proto::ScalarFunction::Nullif => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::NullIf,
                    args,
                },
            };

            Ok(scalar_function)
        }
    }
}

fn from_expr(expr: Expr) -> Result<proto::LogicalExprNode, FieldViolation> {
    use proto::logical_expr_node::ExprType;
    match expr {
        Expr::Column(c) => {
            let expr = proto::LogicalExprNode {
                expr_type: Some(ExprType::Column(from_column(c))),
            };
            Ok(expr)
        }
        Expr::Alias(expr, alias) => {
            let alias = Box::new(proto::AliasNode {
                expr: Some(Box::new(from_expr(*expr)?)),
                alias,
            });
            let expr = proto::LogicalExprNode {
                expr_type: Some(ExprType::Alias(alias)),
            };
            Ok(expr)
        }
        Expr::Literal(value) => {
            let pb_value = from_scalar_value(&value)?;
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::Literal(pb_value)),
            })
        }
        Expr::BinaryExpr { left, op, right } => {
            let binary_expr = Box::new(proto::BinaryExprNode {
                l: Some(Box::new(from_expr(*left)?)),
                r: Some(Box::new(from_expr(*right)?)),
                op: format!("{:?}", op),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::BinaryExpr(binary_expr)),
            })
        }
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            ref window_frame,
        } => {
            let window_function = match fun {
                window_functions::WindowFunction::AggregateFunction(fun) => {
                    proto::window_expr_node::WindowFunction::AggrFunction(
                        from_aggr_function(fun).into(),
                    )
                }
                window_functions::WindowFunction::BuiltInWindowFunction(fun) => {
                    use window_functions::BuiltInWindowFunction;

                    proto::window_expr_node::WindowFunction::BuiltInFunction(
                        match fun {
                            BuiltInWindowFunction::FirstValue => {
                                proto::BuiltInWindowFunction::FirstValue
                            }
                            BuiltInWindowFunction::LastValue => {
                                proto::BuiltInWindowFunction::LastValue
                            }
                            BuiltInWindowFunction::NthValue => {
                                proto::BuiltInWindowFunction::NthValue
                            }
                            BuiltInWindowFunction::Ntile => proto::BuiltInWindowFunction::Ntile,
                            BuiltInWindowFunction::CumeDist => {
                                proto::BuiltInWindowFunction::CumeDist
                            }
                            BuiltInWindowFunction::PercentRank => {
                                proto::BuiltInWindowFunction::PercentRank
                            }
                            BuiltInWindowFunction::RowNumber => {
                                proto::BuiltInWindowFunction::RowNumber
                            }
                            BuiltInWindowFunction::Rank => proto::BuiltInWindowFunction::Rank,
                            BuiltInWindowFunction::Lag => proto::BuiltInWindowFunction::Lag,
                            BuiltInWindowFunction::Lead => proto::BuiltInWindowFunction::Lead,
                            BuiltInWindowFunction::DenseRank => {
                                proto::BuiltInWindowFunction::DenseRank
                            }
                        }
                        .into(),
                    )
                }
            };
            let arg_expr: Option<Box<proto::LogicalExprNode>> = if !args.is_empty() {
                let mut args_iter = args.into_iter();
                let arg = args_iter.next().expect("at least one");
                Some(Box::new(from_expr(arg)?))
            } else {
                None
            };
            let partition_by = partition_by
                .into_iter()
                .map(from_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = order_by
                .into_iter()
                .map(from_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let window_frame = window_frame.map(from_window_frame);
            let window_expr = Box::new(proto::WindowExprNode {
                expr: arg_expr,
                window_function: Some(window_function),
                partition_by,
                order_by,
                window_frame,
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::WindowExpr(window_expr)),
            })
        }
        Expr::AggregateFunction { ref fun, args, .. } => {
            use aggregates::AggregateFunction;
            let aggr_function = match fun {
                AggregateFunction::ApproxDistinct => proto::AggregateFunction::ApproxDistinct,
                AggregateFunction::ApproxPercentileCont => {
                    proto::AggregateFunction::ApproxPercentileCont
                }
                AggregateFunction::ArrayAgg => proto::AggregateFunction::ArrayAgg,
                AggregateFunction::Min => proto::AggregateFunction::Min,
                AggregateFunction::Max => proto::AggregateFunction::Max,
                AggregateFunction::Sum => proto::AggregateFunction::Sum,
                AggregateFunction::Avg => proto::AggregateFunction::Avg,
                AggregateFunction::Count => proto::AggregateFunction::Count,
                AggregateFunction::Variance => proto::AggregateFunction::Variance,
                AggregateFunction::VariancePop => proto::AggregateFunction::VariancePop,
                AggregateFunction::Covariance => proto::AggregateFunction::Covariance,
                AggregateFunction::CovariancePop => proto::AggregateFunction::CovariancePop,
                AggregateFunction::Stddev => proto::AggregateFunction::Stddev,
                AggregateFunction::StddevPop => proto::AggregateFunction::StddevPop,
                AggregateFunction::Correlation => proto::AggregateFunction::Correlation,
                AggregateFunction::ApproxMedian => proto::AggregateFunction::ApproxMedian,
                // need to complete https://github.com/influxdata/influxdb_iox/pull/3997
                // rather than trying to keep up
                AggregateFunction::ApproxPercentileContWithWeight => unimplemented!(),
            };

            let aggregate_expr = proto::AggregateExprNode {
                aggr_function: aggr_function.into(),
                expr: args
                    .into_iter()
                    .map(from_expr)
                    .collect::<Result<Vec<_>, _>>()?,
            };
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
            })
        }
        Expr::ScalarVariable(_, _) => unimplemented!(),
        Expr::ScalarFunction { fun, args } => {
            let fun = from_scalar_function(fun)?;
            let args: Vec<proto::LogicalExprNode> = args
                .into_iter()
                .map(from_expr)
                .collect::<Result<Vec<proto::LogicalExprNode>, FieldViolation>>()?;
            Ok(proto::LogicalExprNode {
                expr_type: Some(proto::logical_expr_node::ExprType::ScalarFunction(
                    proto::ScalarFunctionNode {
                        fun: fun.into(),
                        args,
                    },
                )),
            })
        }
        Expr::ScalarUDF { .. } => unimplemented!(),
        Expr::AggregateUDF { .. } => unimplemented!(),
        Expr::Not(expr) => {
            let expr = Box::new(proto::Not {
                expr: Some(Box::new(from_expr(*expr)?)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::NotExpr(expr)),
            })
        }
        Expr::IsNull(expr) => {
            let expr = Box::new(proto::IsNull {
                expr: Some(Box::new(from_expr(*expr)?)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::IsNullExpr(expr)),
            })
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(proto::IsNotNull {
                expr: Some(Box::new(from_expr(*expr)?)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::IsNotNullExpr(expr)),
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = Box::new(proto::BetweenNode {
                expr: Some(Box::new(from_expr(*expr)?)),
                negated,
                low: Some(Box::new(from_expr(*low)?)),
                high: Some(Box::new(from_expr(*high)?)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::Between(expr)),
            })
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let when_then_expr = when_then_expr
                .into_iter()
                .map(|(w, t)| {
                    Ok(proto::WhenThen {
                        when_expr: Some(from_expr(*w)?),
                        then_expr: Some(from_expr(*t)?),
                    })
                })
                .collect::<Result<Vec<proto::WhenThen>, FieldViolation>>()?;
            let expr = Box::new(proto::CaseNode {
                expr: match expr {
                    Some(e) => Some(Box::new(from_expr(*e)?)),
                    None => None,
                },
                when_then_expr,
                else_expr: match else_expr {
                    Some(e) => Some(Box::new(from_expr(*e)?)),
                    None => None,
                },
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::CaseNode(expr)),
            })
        }
        Expr::Cast { expr, data_type } => {
            let expr = Box::new(proto::CastNode {
                expr: Some(Box::new(from_expr(*expr)?)),
                arrow_type: Some(from_arrow_type(&data_type)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::Cast(expr)),
            })
        }
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => {
            let expr = Box::new(proto::SortExprNode {
                expr: Some(Box::new(from_expr(*expr)?)),
                asc,
                nulls_first,
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(ExprType::Sort(expr)),
            })
        }
        Expr::Negative(expr) => {
            let expr = Box::new(proto::NegativeNode {
                expr: Some(Box::new(from_expr(*expr)?)),
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(proto::logical_expr_node::ExprType::Negative(expr)),
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = Box::new(proto::InListNode {
                expr: Some(Box::new(from_expr(*expr)?)),
                list: list
                    .into_iter()
                    .map(from_expr)
                    .collect::<Result<Vec<_>, FieldViolation>>()?,
                negated,
            });
            Ok(proto::LogicalExprNode {
                expr_type: Some(proto::logical_expr_node::ExprType::InList(expr)),
            })
        }
        Expr::Wildcard => Ok(proto::LogicalExprNode {
            expr_type: Some(proto::logical_expr_node::ExprType::Wildcard(true)),
        }),
        _ => unimplemented!(),
    }
}

fn create_proto_scalar<I, T: FnOnce(&I) -> proto::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: proto::PrimitiveScalarType,
    constructor: T,
) -> proto::ScalarValue {
    proto::ScalarValue {
        value: Some(
            v.as_ref()
                .map(constructor)
                .unwrap_or(proto::scalar_value::Value::NullValue(
                    null_arrow_type as i32,
                )),
        ),
    }
}

fn from_scalar_type(
    value: &datafusion::arrow::datatypes::DataType,
) -> Result<proto::ScalarType, FieldViolation> {
    let datatype = from_data_type(value)?;
    Ok(proto::ScalarType {
        datatype: Some(datatype),
    })
}

fn from_data_type(
    val: &datafusion::arrow::datatypes::DataType,
) -> Result<proto::scalar_type::Datatype, FieldViolation> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use proto::{scalar_type, PrimitiveScalarType};

    let scalar_value = match val {
        DataType::Boolean => scalar_type::Datatype::Scalar(PrimitiveScalarType::Bool as i32),
        DataType::Int8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int8 as i32),
        DataType::Int16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int16 as i32),
        DataType::Int32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int32 as i32),
        DataType::Int64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int64 as i32),
        DataType::UInt8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint8 as i32),
        DataType::UInt16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint16 as i32),
        DataType::UInt32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint32 as i32),
        DataType::UInt64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint64 as i32),
        DataType::Float32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float32 as i32),
        DataType::Float64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float64 as i32),
        DataType::Date32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Date32 as i32),
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Microsecond => {
                scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeMicrosecond as i32)
            }
            TimeUnit::Nanosecond => {
                scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeNanosecond as i32)
            }
            _ => return Err(proto_error("time_unit", "invalid time unit")),
        },
        DataType::Utf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Utf8 as i32),
        DataType::LargeUtf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::LargeUtf8 as i32),
        DataType::List(field_type) => {
            let mut field_names: Vec<String> = Vec::new();
            let mut curr_field = field_type.as_ref();
            field_names.push(curr_field.name().to_owned());
            // For each nested field check nested datatype, since datafusion scalars only support
            // recursive lists with a leaf scalar type
            // any other compound types are errors.

            while let DataType::List(nested_field_type) = curr_field.data_type() {
                curr_field = nested_field_type.as_ref();
                field_names.push(curr_field.name().to_owned());
                if !is_valid_scalar_type_no_list_check(curr_field.data_type()) {
                    return Err(proto_error(
                        curr_field.name(),
                        format!("{:?} is an invalid scalar type", curr_field),
                    ));
                }
            }
            let deepest_datatype = curr_field.data_type();
            if !is_valid_scalar_type_no_list_check(deepest_datatype) {
                return Err(proto_error(
                    curr_field.name(),
                    format!(
                        "The list nested type {:?} is an invalid scalar type",
                        curr_field
                    ),
                ));
            }
            let pb_deepest_type: PrimitiveScalarType = match deepest_datatype {
                DataType::Boolean => PrimitiveScalarType::Bool,
                DataType::Int8 => PrimitiveScalarType::Int8,
                DataType::Int16 => PrimitiveScalarType::Int16,
                DataType::Int32 => PrimitiveScalarType::Int32,
                DataType::Int64 => PrimitiveScalarType::Int64,
                DataType::UInt8 => PrimitiveScalarType::Uint8,
                DataType::UInt16 => PrimitiveScalarType::Uint16,
                DataType::UInt32 => PrimitiveScalarType::Uint32,
                DataType::UInt64 => PrimitiveScalarType::Uint64,
                DataType::Float32 => PrimitiveScalarType::Float32,
                DataType::Float64 => PrimitiveScalarType::Float64,
                DataType::Date32 => PrimitiveScalarType::Date32,
                DataType::Time64(time_unit) => match time_unit {
                    TimeUnit::Microsecond => PrimitiveScalarType::TimeMicrosecond,
                    TimeUnit::Nanosecond => PrimitiveScalarType::TimeNanosecond,
                    _ => {
                        return Err(proto_error(
                            "time unit",
                            "invalid time unit for scalar value",
                        ))
                    }
                },

                DataType::Utf8 => PrimitiveScalarType::Utf8,
                DataType::LargeUtf8 => PrimitiveScalarType::LargeUtf8,
                _ => return Err(proto_error("data_type", "invalid datafusion scalar")),
            };
            proto::scalar_type::Datatype::List(proto::ScalarListType {
                field_names,
                deepest_type: pb_deepest_type as i32,
            })
        }
        DataType::Null
        | DataType::Float16
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::Decimal(_, _) => {
            return Err(proto_error("data_type", "invalid datafusion scalar"))
        }
    };
    Ok(scalar_value)
}

fn from_scalar_value(
    val: &datafusion::scalar::ScalarValue,
) -> Result<proto::ScalarValue, FieldViolation> {
    use datafusion::{arrow::datatypes::DataType, scalar};
    use proto::{scalar_value::Value, PrimitiveScalarType};

    let scalar_val = match val {
        scalar::ScalarValue::Boolean(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Bool, |s| Value::BoolValue(*s))
        }
        scalar::ScalarValue::Float32(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Float32, |s| {
                Value::Float32Value(*s)
            })
        }
        scalar::ScalarValue::Float64(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Float64, |s| {
                Value::Float64Value(*s)
            })
        }
        scalar::ScalarValue::Int8(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Int8, |s| {
                Value::Int8Value(*s as i32)
            })
        }
        scalar::ScalarValue::Int16(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Int16, |s| {
                Value::Int16Value(*s as i32)
            })
        }
        scalar::ScalarValue::Int32(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Int32, |s| Value::Int32Value(*s))
        }
        scalar::ScalarValue::Int64(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Int64, |s| Value::Int64Value(*s))
        }
        scalar::ScalarValue::UInt8(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Uint8, |s| {
                Value::Uint8Value(*s as u32)
            })
        }
        scalar::ScalarValue::UInt16(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Uint16, |s| {
                Value::Uint16Value(*s as u32)
            })
        }
        scalar::ScalarValue::UInt32(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Uint32, |s| Value::Uint32Value(*s))
        }
        scalar::ScalarValue::UInt64(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Uint64, |s| Value::Uint64Value(*s))
        }
        scalar::ScalarValue::Utf8(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Utf8, |s| {
                Value::Utf8Value(s.to_owned())
            })
        }
        scalar::ScalarValue::LargeUtf8(val) => {
            create_proto_scalar(val, PrimitiveScalarType::LargeUtf8, |s| {
                Value::LargeUtf8Value(s.to_owned())
            })
        }
        scalar::ScalarValue::List(value, datatype) => {
            println!("Current datatype of list: {:?}", datatype);
            match value {
                Some(values) => {
                    if values.is_empty() {
                        proto::ScalarValue {
                            value: Some(proto::scalar_value::Value::ListValue(
                                proto::ScalarListValue {
                                    datatype: Some(from_scalar_type(&*datatype)?),
                                    values: Vec::new(),
                                },
                            )),
                        }
                    } else {
                        let scalar_type = match datatype.as_ref() {
                            DataType::List(field) => field.as_ref().data_type(),
                            _ => todo!("Proper error handling"),
                        };
                        println!("Current scalar type for list: {:?}", scalar_type);
                        let type_checked_values: Vec<proto::ScalarValue> = values
                            .iter()
                            .map(|scalar| match (scalar, scalar_type) {
                                (
                                    scalar::ScalarValue::List(_, list_type),
                                    DataType::List(field),
                                ) => {
                                    if let DataType::List(list_field) = list_type.as_ref() {
                                        let scalar_datatype = field.data_type();
                                        let list_datatype = list_field.data_type();
                                        if std::mem::discriminant(list_datatype)
                                            != std::mem::discriminant(scalar_datatype)
                                        {
                                            return Err(proto_error("list", "inconsistent typing"));
                                        }
                                        from_scalar_value(scalar)
                                    } else {
                                        Err(proto_error(
                                            "list",
                                            "inconsistent with designated type",
                                        ))
                                    }
                                }
                                (scalar::ScalarValue::Boolean(_), DataType::Boolean) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Float32(_), DataType::Float32) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Float64(_), DataType::Float64) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Int8(_), DataType::Int8) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Int16(_), DataType::Int16) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Int32(_), DataType::Int32) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Int64(_), DataType::Int64) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::UInt8(_), DataType::UInt8) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::UInt16(_), DataType::UInt16) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::UInt32(_), DataType::UInt32) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::UInt64(_), DataType::UInt64) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::Utf8(_), DataType::Utf8) => {
                                    from_scalar_value(scalar)
                                }
                                (scalar::ScalarValue::LargeUtf8(_), DataType::LargeUtf8) => {
                                    from_scalar_value(scalar)
                                }
                                _ => Err(proto_error("list", "inconsistent with designated type")),
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        proto::ScalarValue {
                            value: Some(proto::scalar_value::Value::ListValue(
                                proto::ScalarListValue {
                                    datatype: Some(from_scalar_type(&*datatype)?),
                                    values: type_checked_values,
                                },
                            )),
                        }
                    }
                }
                None => proto::ScalarValue {
                    value: Some(proto::scalar_value::Value::NullListValue(from_scalar_type(
                        &*datatype,
                    )?)),
                },
            }
        }
        datafusion::scalar::ScalarValue::Date32(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Date32, |s| Value::Date32Value(*s))
        }
        datafusion::scalar::ScalarValue::TimestampMicrosecond(val, _) => {
            create_proto_scalar(val, PrimitiveScalarType::TimeMicrosecond, |s| {
                Value::TimeMicrosecondValue(*s)
            })
        }
        datafusion::scalar::ScalarValue::TimestampNanosecond(val, _) => {
            create_proto_scalar(val, PrimitiveScalarType::TimeNanosecond, |s| {
                Value::TimeNanosecondValue(*s)
            })
        }
        datafusion::scalar::ScalarValue::Decimal128(val, p, s) => match val {
            Some(v) => {
                let array = v.to_be_bytes();
                let vec_val: Vec<u8> = array.to_vec();
                proto::ScalarValue {
                    value: Some(Value::Decimal128Value(proto::Decimal128 {
                        value: vec_val,
                        p: *p as i64,
                        s: *s as i64,
                    })),
                }
            }
            None => proto::ScalarValue {
                value: Some(proto::scalar_value::Value::NullValue(
                    PrimitiveScalarType::Decimal128 as i32,
                )),
            },
        },
        datafusion::scalar::ScalarValue::Date64(val) => {
            create_proto_scalar(val, PrimitiveScalarType::Date64, |s| Value::Date64Value(*s))
        }
        datafusion::scalar::ScalarValue::TimestampSecond(val, _) => {
            create_proto_scalar(val, PrimitiveScalarType::TimeSecond, |s| {
                Value::TimeSecondValue(*s)
            })
        }
        datafusion::scalar::ScalarValue::TimestampMillisecond(val, _) => {
            create_proto_scalar(val, PrimitiveScalarType::TimeMillisecond, |s| {
                Value::TimeMillisecondValue(*s)
            })
        }
        datafusion::scalar::ScalarValue::IntervalYearMonth(val) => {
            create_proto_scalar(val, PrimitiveScalarType::IntervalYearmonth, |s| {
                Value::IntervalYearmonthValue(*s)
            })
        }
        datafusion::scalar::ScalarValue::IntervalDayTime(val) => {
            create_proto_scalar(val, PrimitiveScalarType::IntervalDaytime, |s| {
                Value::IntervalDaytimeValue(*s)
            })
        }
        _ => return Err(proto_error("data_type", "invalid datafusion scalar")),
    };
    Ok(scalar_val)
}

fn from_proto_scalar_value(
    proto: proto::ScalarValue,
) -> Result<datafusion::scalar::ScalarValue, FieldViolation> {
    use datafusion::scalar::ScalarValue;
    use proto::scalar_value::Value::*;

    let value = proto.value.unwrap_field("value")?;

    let result = match value {
        BoolValue(v) => ScalarValue::Boolean(Some(v)),
        Utf8Value(v) => ScalarValue::Utf8(Some(v)),
        LargeUtf8Value(v) => ScalarValue::LargeUtf8(Some(v)),
        Int8Value(v) => ScalarValue::Int8(Some(v as i8)),
        Int16Value(v) => ScalarValue::Int16(Some(v as i16)),
        Int32Value(v) => ScalarValue::Int32(Some(v)),
        Int64Value(v) => ScalarValue::Int64(Some(v)),
        Uint8Value(v) => ScalarValue::UInt8(Some(v as u8)),
        Uint16Value(v) => ScalarValue::UInt16(Some(v as u16)),
        Uint32Value(v) => ScalarValue::UInt32(Some(v)),
        Uint64Value(v) => ScalarValue::UInt64(Some(v)),
        Float32Value(v) => ScalarValue::Float32(Some(v)),
        Float64Value(v) => ScalarValue::Float64(Some(v)),
        Date32Value(v) => ScalarValue::Date32(Some(v)),
        TimeMicrosecondValue(v) => ScalarValue::TimestampMicrosecond(Some(v), None),
        TimeNanosecondValue(v) => ScalarValue::TimestampNanosecond(Some(v), None),
        ListValue(_v) => unimplemented!("this is nontrivial"),
        NullListValue(_v) => unimplemented!("this is nontrivial"),
        NullValue(_null_enum) => unimplemented!("this is nontrivial"),
        Decimal128Value(_v) => unimplemented!("this is nontrivial"),
        Date64Value(v) => ScalarValue::Date64(Some(v)),
        TimeSecondValue(v) => ScalarValue::TimestampSecond(Some(v), None),
        TimeMillisecondValue(v) => ScalarValue::TimestampMillisecond(Some(v), None),
        IntervalYearmonthValue(v) => ScalarValue::IntervalYearMonth(Some(v)),
        IntervalDaytimeValue(v) => ScalarValue::IntervalDayTime(Some(v)),
    };

    Ok(result)
}

fn from_aggr_function(fun: aggregates::AggregateFunction) -> proto::AggregateFunction {
    use aggregates::AggregateFunction;
    match fun {
        AggregateFunction::ApproxDistinct => proto::AggregateFunction::ApproxDistinct,
        AggregateFunction::ApproxPercentileCont => proto::AggregateFunction::ApproxPercentileCont,
        AggregateFunction::ArrayAgg => proto::AggregateFunction::ArrayAgg,
        AggregateFunction::Min => proto::AggregateFunction::Min,
        AggregateFunction::Max => proto::AggregateFunction::Max,
        AggregateFunction::Sum => proto::AggregateFunction::Sum,
        AggregateFunction::Avg => proto::AggregateFunction::Avg,
        AggregateFunction::Count => proto::AggregateFunction::Count,
        AggregateFunction::Variance => proto::AggregateFunction::Variance,
        AggregateFunction::VariancePop => proto::AggregateFunction::VariancePop,
        AggregateFunction::Covariance => proto::AggregateFunction::Covariance,
        AggregateFunction::CovariancePop => proto::AggregateFunction::CovariancePop,
        AggregateFunction::Stddev => proto::AggregateFunction::Stddev,
        AggregateFunction::StddevPop => proto::AggregateFunction::StddevPop,
        AggregateFunction::Correlation => proto::AggregateFunction::Correlation,
        AggregateFunction::ApproxMedian => proto::AggregateFunction::ApproxMedian,
        // need to complete https://github.com/influxdata/influxdb_iox/pull/3997
        // rather than trying to keep up
        AggregateFunction::ApproxPercentileContWithWeight => unimplemented!(),
    }
}

fn from_scalar_function(
    fun: functions::BuiltinScalarFunction,
) -> Result<proto::ScalarFunction, FieldViolation> {
    use functions::BuiltinScalarFunction;
    match fun {
        BuiltinScalarFunction::Sqrt => Ok(proto::ScalarFunction::Sqrt),
        BuiltinScalarFunction::Sin => Ok(proto::ScalarFunction::Sin),
        BuiltinScalarFunction::Cos => Ok(proto::ScalarFunction::Cos),
        BuiltinScalarFunction::Tan => Ok(proto::ScalarFunction::Tan),
        BuiltinScalarFunction::Asin => Ok(proto::ScalarFunction::Asin),
        BuiltinScalarFunction::Acos => Ok(proto::ScalarFunction::Acos),
        BuiltinScalarFunction::Atan => Ok(proto::ScalarFunction::Atan),
        BuiltinScalarFunction::Exp => Ok(proto::ScalarFunction::Exp),
        BuiltinScalarFunction::Log => Ok(proto::ScalarFunction::Log),
        BuiltinScalarFunction::Ln => Ok(proto::ScalarFunction::Ln),
        BuiltinScalarFunction::Log10 => Ok(proto::ScalarFunction::Log10),
        BuiltinScalarFunction::Floor => Ok(proto::ScalarFunction::Floor),
        BuiltinScalarFunction::Ceil => Ok(proto::ScalarFunction::Ceil),
        BuiltinScalarFunction::Round => Ok(proto::ScalarFunction::Round),
        BuiltinScalarFunction::Trunc => Ok(proto::ScalarFunction::Trunc),
        BuiltinScalarFunction::Abs => Ok(proto::ScalarFunction::Abs),
        BuiltinScalarFunction::OctetLength => Ok(proto::ScalarFunction::Octetlength),
        BuiltinScalarFunction::Concat => Ok(proto::ScalarFunction::Concat),
        BuiltinScalarFunction::Lower => Ok(proto::ScalarFunction::Lower),
        BuiltinScalarFunction::Upper => Ok(proto::ScalarFunction::Upper),
        BuiltinScalarFunction::Trim => Ok(proto::ScalarFunction::Trim),
        BuiltinScalarFunction::Ltrim => Ok(proto::ScalarFunction::Ltrim),
        BuiltinScalarFunction::Rtrim => Ok(proto::ScalarFunction::Rtrim),
        BuiltinScalarFunction::ToTimestamp => Ok(proto::ScalarFunction::Totimestamp),
        BuiltinScalarFunction::Array => Ok(proto::ScalarFunction::Array),
        BuiltinScalarFunction::NullIf => Ok(proto::ScalarFunction::Nullif),
        BuiltinScalarFunction::DatePart => Ok(proto::ScalarFunction::Datepart),
        BuiltinScalarFunction::DateTrunc => Ok(proto::ScalarFunction::Datetrunc),
        BuiltinScalarFunction::MD5 => Ok(proto::ScalarFunction::Md5),
        BuiltinScalarFunction::SHA224 => Ok(proto::ScalarFunction::Sha224),
        BuiltinScalarFunction::SHA256 => Ok(proto::ScalarFunction::Sha256),
        BuiltinScalarFunction::SHA384 => Ok(proto::ScalarFunction::Sha384),
        BuiltinScalarFunction::SHA512 => Ok(proto::ScalarFunction::Sha512),
        BuiltinScalarFunction::Digest => Ok(proto::ScalarFunction::Digest),
        BuiltinScalarFunction::ToTimestampMillis => Ok(proto::ScalarFunction::Totimestampmillis),
        _ => Err(proto_error("scalar_function", "unsupported")),
    }
}

fn from_proto_aggr_function(
    proto: proto::AggregateFunction,
) -> Result<aggregates::AggregateFunction, FieldViolation> {
    use aggregates::AggregateFunction;
    let agg = match proto {
        proto::AggregateFunction::Unspecified => {
            return Err(proto_error("aggregate_function", "not specified"))
        }
        proto::AggregateFunction::Min => AggregateFunction::Min,
        proto::AggregateFunction::Max => AggregateFunction::Max,
        proto::AggregateFunction::Sum => AggregateFunction::Sum,
        proto::AggregateFunction::Avg => AggregateFunction::Avg,
        proto::AggregateFunction::Count => AggregateFunction::Count,
        proto::AggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
        proto::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
        proto::AggregateFunction::Variance => AggregateFunction::Variance,
        proto::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
        proto::AggregateFunction::Covariance => AggregateFunction::Covariance,
        proto::AggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
        proto::AggregateFunction::Stddev => AggregateFunction::Stddev,
        proto::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
        proto::AggregateFunction::Correlation => AggregateFunction::Correlation,
        proto::AggregateFunction::ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
        proto::AggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
    };

    Ok(agg)
}

fn from_window_frame(window_frame: WindowFrame) -> proto::WindowFrame {
    let units = match window_frame.units {
        WindowFrameUnits::Rows => proto::WindowFrameUnits::Rows,
        WindowFrameUnits::Range => proto::WindowFrameUnits::Range,
        WindowFrameUnits::Groups => proto::WindowFrameUnits::Groups,
    };

    proto::WindowFrame {
        window_frame_units: units.into(),
        start_bound: Some(from_window_frame_bound(&window_frame.start_bound)),
        end_bound: Some(from_window_frame_bound(&window_frame.end_bound)),
    }
}

fn from_window_frame_bound(bound: &WindowFrameBound) -> proto::WindowFrameBound {
    match bound {
        WindowFrameBound::CurrentRow => proto::WindowFrameBound {
            window_frame_bound_type: proto::WindowFrameBoundType::CurrentRow.into(),
            bound_value: None,
        },
        WindowFrameBound::Preceding(v) => proto::WindowFrameBound {
            window_frame_bound_type: proto::WindowFrameBoundType::Preceding.into(),
            bound_value: *v,
        },
        WindowFrameBound::Following(v) => proto::WindowFrameBound {
            window_frame_bound_type: proto::WindowFrameBoundType::Following.into(),
            bound_value: *v,
        },
    }
}

fn from_proto_window_frame(proto: proto::WindowFrame) -> Result<WindowFrame, FieldViolation> {
    let units = proto.window_frame_units();
    let units = match units {
        proto::WindowFrameUnits::Unspecified => {
            return Err(proto_error("window_frame_units", "not specified"))
        }
        proto::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
        proto::WindowFrameUnits::Range => WindowFrameUnits::Range,
        proto::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
    };

    let start_bound =
        from_proto_window_frame_bound(proto.start_bound.unwrap_field("start_bound")?)?;

    let end_bound: Option<Result<_, FieldViolation>> =
        proto.end_bound.map(from_proto_window_frame_bound);
    let end_bound = end_bound
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);

    Ok(WindowFrame {
        units,
        start_bound,
        end_bound,
    })
}

fn from_proto_window_frame_bound(
    proto: proto::WindowFrameBound,
) -> Result<WindowFrameBound, FieldViolation> {
    let bound_type = proto.window_frame_bound_type();

    let bound = match bound_type {
        proto::WindowFrameBoundType::Unspecified => {
            return Err(proto_error("window_frame_bound_type", "not specified"))
        }
        proto::WindowFrameBoundType::CurrentRow => WindowFrameBound::CurrentRow,
        proto::WindowFrameBoundType::Preceding => WindowFrameBound::Preceding(proto.bound_value),
        proto::WindowFrameBoundType::Following => WindowFrameBound::Following(proto.bound_value),
    };

    Ok(bound)
}

fn from_proto_interval_unit(
    proto: proto::IntervalUnit,
) -> Result<datafusion::arrow::datatypes::IntervalUnit, FieldViolation> {
    use datafusion::arrow::datatypes::IntervalUnit;

    let iu = match proto {
        proto::IntervalUnit::Unspecified => {
            return Err(proto_error("interval_unit", "not specified"))
        }
        proto::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
        proto::IntervalUnit::DayTime => IntervalUnit::DayTime,
        proto::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
    };

    Ok(iu)
}

fn from_proto_time_unit(
    proto: proto::TimeUnit,
) -> Result<datafusion::arrow::datatypes::TimeUnit, FieldViolation> {
    use datafusion::arrow::datatypes::TimeUnit;

    let unit = match proto {
        proto::TimeUnit::Unspecified => return Err(proto_error("time_unit", "not specified")),
        proto::TimeUnit::Second => TimeUnit::Second,
        proto::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
        proto::TimeUnit::Microsecond => TimeUnit::Microsecond,
        proto::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
    };

    Ok(unit)
}

fn from_arrow_type(arrow: &datafusion::arrow::datatypes::DataType) -> proto::ArrowType {
    proto::ArrowType {
        arrow_type_enum: Some(from_inner_arrow_type(arrow)),
    }
}

fn from_inner_arrow_type(
    val: &datafusion::arrow::datatypes::DataType,
) -> proto::arrow_type::ArrowTypeEnum {
    use datafusion::arrow::datatypes::{DataType, UnionMode};
    use proto::{arrow_type::ArrowTypeEnum, EmptyMessage};

    match val {
        DataType::Null => ArrowTypeEnum::None(EmptyMessage {}),
        DataType::Boolean => ArrowTypeEnum::Bool(EmptyMessage {}),
        DataType::Int8 => ArrowTypeEnum::Int8(EmptyMessage {}),
        DataType::Int16 => ArrowTypeEnum::Int16(EmptyMessage {}),
        DataType::Int32 => ArrowTypeEnum::Int32(EmptyMessage {}),
        DataType::Int64 => ArrowTypeEnum::Int64(EmptyMessage {}),
        DataType::UInt8 => ArrowTypeEnum::Uint8(EmptyMessage {}),
        DataType::UInt16 => ArrowTypeEnum::Uint16(EmptyMessage {}),
        DataType::UInt32 => ArrowTypeEnum::Uint32(EmptyMessage {}),
        DataType::UInt64 => ArrowTypeEnum::Uint64(EmptyMessage {}),
        DataType::Float16 => ArrowTypeEnum::Float16(EmptyMessage {}),
        DataType::Float32 => ArrowTypeEnum::Float32(EmptyMessage {}),
        DataType::Float64 => ArrowTypeEnum::Float64(EmptyMessage {}),
        DataType::Timestamp(time_unit, timezone) => ArrowTypeEnum::Timestamp(proto::Timestamp {
            time_unit: from_arrow_time_unit(time_unit).into(),
            timezone: timezone.to_owned().unwrap_or_default(),
        }),
        DataType::Date32 => ArrowTypeEnum::Date32(EmptyMessage {}),
        DataType::Date64 => ArrowTypeEnum::Date64(EmptyMessage {}),
        DataType::Time32(time_unit) => {
            ArrowTypeEnum::Time32(from_arrow_time_unit(time_unit) as i32)
        }
        DataType::Time64(time_unit) => {
            ArrowTypeEnum::Time64(from_arrow_time_unit(time_unit) as i32)
        }
        DataType::Duration(time_unit) => {
            ArrowTypeEnum::Duration(from_arrow_time_unit(time_unit) as i32)
        }
        DataType::Interval(interval_unit) => {
            ArrowTypeEnum::Interval(from_arrow_interval_unit(interval_unit) as i32)
        }
        DataType::Binary => ArrowTypeEnum::Binary(EmptyMessage {}),
        DataType::FixedSizeBinary(size) => ArrowTypeEnum::FixedSizeBinary(*size),
        DataType::LargeBinary => ArrowTypeEnum::LargeBinary(EmptyMessage {}),
        DataType::Utf8 => ArrowTypeEnum::Utf8(EmptyMessage {}),
        DataType::LargeUtf8 => ArrowTypeEnum::LargeUtf8(EmptyMessage {}),
        DataType::List(item_type) => ArrowTypeEnum::List(Box::new(proto::List {
            field_type: Some(Box::new(from_field(&*item_type))),
        })),
        DataType::FixedSizeList(item_type, list_size) => {
            ArrowTypeEnum::FixedSizeList(Box::new(proto::FixedSizeList {
                field_type: Some(Box::new(from_field(&*item_type))),
                list_size: *list_size,
            }))
        }
        DataType::LargeList(item_type) => ArrowTypeEnum::LargeList(Box::new(proto::List {
            field_type: Some(Box::new(from_field(&*item_type))),
        })),
        DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(proto::Struct {
            sub_field_types: struct_fields.iter().map(from_field).collect::<Vec<_>>(),
        }),
        DataType::Union(union_types, union_mode) => {
            let union_mode = match union_mode {
                UnionMode::Sparse => proto::UnionMode::Sparse,
                UnionMode::Dense => proto::UnionMode::Dense,
            };
            ArrowTypeEnum::Union(proto::Union {
                union_types: union_types.iter().map(from_field).collect::<Vec<_>>(),
                union_mode: union_mode.into(),
            })
        }
        DataType::Dictionary(key_type, value_type) => {
            ArrowTypeEnum::Dictionary(Box::new(proto::Dictionary {
                key: Some(Box::new(from_arrow_type(&*key_type))),
                value: Some(Box::new(from_arrow_type(&*value_type))),
            }))
        }
        DataType::Decimal(whole, fractional) => ArrowTypeEnum::Decimal(proto::Decimal {
            whole: *whole as u64,
            fractional: *fractional as u64,
        }),
        DataType::Map(_, _) => {
            unimplemented!("Map data type not yet supported")
        }
    }
}

fn from_field(field: &datafusion::arrow::datatypes::Field) -> proto::Field {
    proto::Field {
        name: field.name().to_owned(),
        arrow_type: Some(Box::new(from_arrow_type(field.data_type()))),
        nullable: field.is_nullable(),
        children: Vec::new(),
    }
}

fn from_arrow_time_unit(val: &datafusion::arrow::datatypes::TimeUnit) -> proto::TimeUnit {
    use datafusion::arrow::datatypes::TimeUnit;
    match val {
        TimeUnit::Second => proto::TimeUnit::Second,
        TimeUnit::Millisecond => proto::TimeUnit::TimeMillisecond,
        TimeUnit::Microsecond => proto::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => proto::TimeUnit::Nanosecond,
    }
}

fn from_arrow_interval_unit(
    interval_unit: &datafusion::arrow::datatypes::IntervalUnit,
) -> proto::IntervalUnit {
    use datafusion::arrow::datatypes::IntervalUnit;

    match interval_unit {
        IntervalUnit::YearMonth => proto::IntervalUnit::YearMonth,
        IntervalUnit::DayTime => proto::IntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => proto::IntervalUnit::MonthDayNano,
    }
}

//Does not check if list subtypes are valid
fn is_valid_scalar_type_no_list_check(datatype: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    match datatype {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Date32 => true,
        DataType::Time64(time_unit) => {
            matches!(time_unit, TimeUnit::Microsecond | TimeUnit::Nanosecond)
        }

        DataType::List(_) => true,
        _ => false,
    }
}

fn from_proto_arrow_type(
    proto: proto::arrow_type::ArrowTypeEnum,
) -> Result<datafusion::arrow::datatypes::DataType, FieldViolation> {
    use datafusion::arrow::datatypes::{DataType, UnionMode};
    use proto::arrow_type::ArrowTypeEnum;

    let ty = match proto {
        ArrowTypeEnum::None(_) => DataType::Null,
        ArrowTypeEnum::Bool(_) => DataType::Boolean,
        ArrowTypeEnum::Uint8(_) => DataType::UInt8,
        ArrowTypeEnum::Int8(_) => DataType::Int8,
        ArrowTypeEnum::Uint16(_) => DataType::UInt16,
        ArrowTypeEnum::Int16(_) => DataType::Int16,
        ArrowTypeEnum::Uint32(_) => DataType::UInt32,
        ArrowTypeEnum::Int32(_) => DataType::Int32,
        ArrowTypeEnum::Uint64(_) => DataType::UInt64,
        ArrowTypeEnum::Int64(_) => DataType::Int64,
        ArrowTypeEnum::Float16(_) => DataType::Float16,
        ArrowTypeEnum::Float32(_) => DataType::Float32,
        ArrowTypeEnum::Float64(_) => DataType::Float64,
        ArrowTypeEnum::Utf8(_) => DataType::Utf8,
        ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
        ArrowTypeEnum::Binary(_) => DataType::Binary,
        ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(size),
        ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
        ArrowTypeEnum::Date32(_) => DataType::Date32,
        ArrowTypeEnum::Date64(_) => DataType::Date64,
        ArrowTypeEnum::Duration(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Duration(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Timestamp(proto::Timestamp {
            time_unit,
            timezone,
        }) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Timestamp(
                from_proto_time_unit(time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone),
                },
            )
        }
        ArrowTypeEnum::Time32(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Time32(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Time64(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Time64(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Interval(interval_unit) => {
            let interval_unit =
                proto::IntervalUnit::from_i32(interval_unit).unwrap_field("interval_unit")?;
            DataType::Interval(from_proto_interval_unit(interval_unit)?)
        }
        ArrowTypeEnum::Decimal(proto::Decimal { whole, fractional }) => {
            DataType::Decimal(whole as usize, fractional as usize)
        }
        ArrowTypeEnum::List(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::List(Box::new(from_proto_field(*list_type)?))
        }
        ArrowTypeEnum::LargeList(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::LargeList(Box::new(from_proto_field(*list_type)?))
        }
        ArrowTypeEnum::FixedSizeList(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::FixedSizeList(Box::new(from_proto_field(*list_type)?), list.list_size)
        }
        ArrowTypeEnum::Struct(strct) => DataType::Struct(
            strct
                .sub_field_types
                .into_iter()
                .map(from_proto_field)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ArrowTypeEnum::Union(union) => {
            let union_mode =
                proto::UnionMode::from_i32(union.union_mode).unwrap_field("union_mode")?;
            let union_mode = match union_mode {
                proto::UnionMode::Unspecified => {
                    return Err(proto_error("union_mode", "not specified"))
                }
                proto::UnionMode::Dense => UnionMode::Dense,
                proto::UnionMode::Sparse => UnionMode::Sparse,
            };
            let union_types = union
                .union_types
                .into_iter()
                .map(from_proto_field)
                .collect::<Result<Vec<_>, _>>()?;
            DataType::Union(union_types, union_mode)
        }
        ArrowTypeEnum::Dictionary(dict) => {
            let key_datatype = dict
                .key
                .map(|t| from_proto_arrow_type(t.arrow_type_enum.unwrap_field("arrow_type_enum")?))
                .transpose()?
                .unwrap_field("key")?;
            let value_datatype = dict
                .value
                .map(|t| from_proto_arrow_type(t.arrow_type_enum.unwrap_field("arrow_type_enum")?))
                .transpose()?
                .unwrap_field("value")?;
            DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
        }
    };

    Ok(ty)
}

fn from_proto_field(
    proto: proto::Field,
) -> Result<datafusion::arrow::datatypes::Field, FieldViolation> {
    let data_type = proto
        .arrow_type
        .unwrap_field("arrow_type")?
        .arrow_type_enum
        .unwrap_field("arrow_type_enum")?;
    let data_type = from_proto_arrow_type(data_type)?;
    Ok(datafusion::arrow::datatypes::Field::new(
        &proto.name,
        data_type,
        proto.nullable,
    ))
}

fn proto_error(field: impl Into<String>, description: impl Into<String>) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: description.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::col;

    #[test]
    fn query_from_protobuf() {
        let rust_predicate = predicate::PredicateBuilder::new()
            .timestamp_range(1, 100)
            .add_expr(col("foo"))
            .build();

        let proto_predicate = proto::Predicate {
            exprs: vec![proto::LogicalExprNode {
                expr_type: Some(proto::logical_expr_node::ExprType::Column(proto::Column {
                    name: "foo".into(),
                    relation: None,
                })),
            }],
            field_columns: vec![],
            partition_key: None,
            range: Some(proto::TimestampRange { start: 1, end: 100 }),
            value_expr: vec![],
        };

        let rust_query = IngesterQueryRequest::new(
            "mydb".into(),
            SequencerId::new(5),
            "cpu".into(),
            vec!["usage".into(), "time".into()],
            Some(rust_predicate),
        );

        let proto_query = proto::IngesterQueryRequest {
            namespace: "mydb".into(),
            sequencer_id: 5,
            table: "cpu".into(),
            columns: vec!["usage".into(), "time".into()],
            predicate: Some(proto_predicate),
        };

        let rust_query_converted = IngesterQueryRequest::try_from(proto_query).unwrap();

        assert_eq!(rust_query, rust_query_converted);
    }
}
