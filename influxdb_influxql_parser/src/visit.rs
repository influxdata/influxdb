//! The visit module provides API for walking the AST.
//!
//! # Example
//!
//! ```
//! use influxdb_influxql_parser::visit::{Visitable, Visitor};
//! use influxdb_influxql_parser::parse_statements;
//! use influxdb_influxql_parser::common::WhereClause;
//!
//! struct MyVisitor;
//!
//! impl Visitor for MyVisitor {
//!     type Error = ();
//!
//!     fn post_visit_where_clause(self, n: &WhereClause) -> Result<Self, Self::Error> {
//!         println!("{}", n);
//!         Ok(self)
//!     }
//! }
//!
//! let statements = parse_statements("SELECT value FROM cpu WHERE host = 'west'").unwrap();
//! let statement  = statements.first().unwrap();
//! let vis = MyVisitor;
//! statement.accept(vis);
//! ```
use self::Recursion::*;
use crate::common::{
    LimitClause, MeasurementName, OffsetClause, OrderByClause, QualifiedMeasurementName,
    WhereClause,
};
use crate::create::CreateDatabaseStatement;
use crate::delete::DeleteStatement;
use crate::drop::DropMeasurementStatement;
use crate::explain::ExplainStatement;
use crate::expression::arithmetic::Expr;
use crate::expression::conditional::ConditionalExpression;
use crate::expression::{Binary, Call, ConditionalBinary, VarRef};
use crate::literal::Literal;
use crate::select::{
    Dimension, Field, FieldList, FillClause, FromMeasurementClause, GroupByClause,
    MeasurementSelection, SLimitClause, SOffsetClause, SelectStatement, TimeDimension,
    TimeZoneClause,
};
use crate::show::{OnClause, ShowDatabasesStatement};
use crate::show_field_keys::ShowFieldKeysStatement;
use crate::show_measurements::{
    ExtendedOnClause, ShowMeasurementsStatement, WithMeasurementClause,
};
use crate::show_retention_policies::ShowRetentionPoliciesStatement;
use crate::show_tag_keys::ShowTagKeysStatement;
use crate::show_tag_values::{ShowTagValuesStatement, WithKeyClause};
use crate::simple_from_clause::{DeleteFromClause, ShowFromClause};
use crate::statement::Statement;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: Visitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the depth-first traversal of an InfluxQL statement. When passed to
/// any [`Visitable::accept`], `pre_visit` functions are invoked repeatedly
/// until a leaf node is reached or a `pre_visit` function returns [`Recursion::Stop`].
pub trait Visitor: Sized {
    /// The type returned in the event of an error traversing the tree.
    type Error;

    /// Invoked before any children of the InfluxQL statement are visited.
    fn pre_visit_statement(self, _n: &Statement) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the InfluxQL statement are visited.
    fn post_visit_statement(self, _n: &Statement) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of `n` are visited.
    fn pre_visit_create_database_statement(
        self,
        _n: &CreateDatabaseStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of `n` are visited. Default
    /// implementation does nothing.
    fn post_visit_create_database_statement(
        self,
        _n: &CreateDatabaseStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `DELETE` statement are visited.
    fn pre_visit_delete_statement(
        self,
        _n: &DeleteStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `DELETE` statement are visited.
    fn post_visit_delete_statement(self, _n: &DeleteStatement) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause of a `DELETE` statement are visited.
    fn pre_visit_delete_from_clause(
        self,
        _n: &DeleteFromClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause of a `DELETE` statement are visited.
    fn post_visit_delete_from_clause(self, _n: &DeleteFromClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the measurement name are visited.
    fn pre_visit_measurement_name(
        self,
        _n: &MeasurementName,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the measurement name are visited.
    fn post_visit_measurement_name(self, _n: &MeasurementName) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `DROP MEASUREMENT` statement are visited.
    fn pre_visit_drop_measurement_statement(
        self,
        _n: &DropMeasurementStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `DROP MEASUREMENT` statement are visited.
    fn post_visit_drop_measurement_statement(
        self,
        _n: &DropMeasurementStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `EXPLAIN` statement are visited.
    fn pre_visit_explain_statement(
        self,
        _n: &ExplainStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `EXPLAIN` statement are visited.
    fn post_visit_explain_statement(self, _n: &ExplainStatement) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SELECT` statement are visited.
    fn pre_visit_select_statement(
        self,
        _n: &SelectStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SELECT` statement are visited.
    fn post_visit_select_statement(self, _n: &SelectStatement) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW DATABASES` statement are visited.
    fn pre_visit_show_databases_statement(
        self,
        _n: &ShowDatabasesStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW DATABASES` statement are visited.
    fn post_visit_show_databases_statement(
        self,
        _n: &ShowDatabasesStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW MEASUREMENTS` statement are visited.
    fn pre_visit_show_measurements_statement(
        self,
        _n: &ShowMeasurementsStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW MEASUREMENTS` statement are visited.
    fn post_visit_show_measurements_statement(
        self,
        _n: &ShowMeasurementsStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW RETENTION POLICIES` statement are visited.
    fn pre_visit_show_retention_policies_statement(
        self,
        _n: &ShowRetentionPoliciesStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW RETENTION POLICIES` statement are visited.
    fn post_visit_show_retention_policies_statement(
        self,
        _n: &ShowRetentionPoliciesStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW TAG KEYS` statement are visited.
    fn pre_visit_show_tag_keys_statement(
        self,
        _n: &ShowTagKeysStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW TAG KEYS` statement are visited.
    fn post_visit_show_tag_keys_statement(
        self,
        _n: &ShowTagKeysStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW TAG VALUES` statement are visited.
    fn pre_visit_show_tag_values_statement(
        self,
        _n: &ShowTagValuesStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW TAG VALUES` statement are visited.
    fn post_visit_show_tag_values_statement(
        self,
        _n: &ShowTagValuesStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW FIELD KEYS` statement are visited.
    fn pre_visit_show_field_keys_statement(
        self,
        _n: &ShowFieldKeysStatement,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW FIELD KEYS` statement are visited.
    fn post_visit_show_field_keys_statement(
        self,
        _n: &ShowFieldKeysStatement,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the conditional expression are visited.
    fn pre_visit_conditional_expression(
        self,
        _n: &ConditionalExpression,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the conditional expression are visited.
    fn post_visit_conditional_expression(
        self,
        _n: &ConditionalExpression,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the arithmetic expression are visited.
    fn pre_visit_expr(self, _n: &Expr) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the arithmetic expression are visited.
    fn post_visit_expr(self, _n: &Expr) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any fields of the `SELECT` projection are visited.
    fn pre_visit_select_field_list(self, _n: &FieldList) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all fields of the `SELECT` projection are visited.
    fn post_visit_select_field_list(self, _n: &FieldList) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the field of a `SELECT` statement are visited.
    fn pre_visit_select_field(self, _n: &Field) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the field of a `SELECT` statement are visited.
    fn post_visit_select_field(self, _n: &Field) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause of a `SELECT` statement are visited.
    fn pre_visit_select_from_clause(
        self,
        _n: &FromMeasurementClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause of a `SELECT` statement are visited.
    fn post_visit_select_from_clause(
        self,
        _n: &FromMeasurementClause,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn pre_visit_select_measurement_selection(
        self,
        _n: &MeasurementSelection,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn post_visit_select_measurement_selection(
        self,
        _n: &MeasurementSelection,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `GROUP BY` clause are visited.
    fn pre_visit_group_by_clause(self, _n: &GroupByClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `GROUP BY` clause are visited.
    fn post_visit_group_by_clause(self, _n: &GroupByClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `GROUP BY` dimension expression are visited.
    fn pre_visit_select_dimension(self, _n: &Dimension) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `GROUP BY` dimension expression are visited.
    fn post_visit_select_dimension(self, _n: &Dimension) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before `TIME` dimension clause is visited.
    fn pre_visit_select_time_dimension(
        self,
        _n: &TimeDimension,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after `TIME` dimension clause is visited.
    fn post_visit_select_time_dimension(self, _n: &TimeDimension) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `WHERE` clause are visited.
    fn pre_visit_where_clause(self, _n: &WhereClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `WHERE` clause are visited.
    fn post_visit_where_clause(self, _n: &WhereClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause for any `SHOW` statement are visited.
    fn pre_visit_show_from_clause(
        self,
        _n: &ShowFromClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause for any `SHOW` statement are visited.
    fn post_visit_show_from_clause(self, _n: &ShowFromClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the qualified measurement name are visited.
    fn pre_visit_qualified_measurement_name(
        self,
        _n: &QualifiedMeasurementName,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the qualified measurement name are visited.
    fn post_visit_qualified_measurement_name(
        self,
        _n: &QualifiedMeasurementName,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `FILL` clause are visited.
    fn pre_visit_fill_clause(self, _n: &FillClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FILL` clause are visited.
    fn post_visit_fill_clause(self, _n: &FillClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `ORDER BY` clause are visited.
    fn pre_visit_order_by_clause(self, _n: &OrderByClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `ORDER BY` clause are visited.
    fn post_visit_order_by_clause(self, _n: &OrderByClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `LIMIT` clause are visited.
    fn pre_visit_limit_clause(self, _n: &LimitClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `LIMIT` clause are visited.
    fn post_visit_limit_clause(self, _n: &LimitClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `OFFSET` clause are visited.
    fn pre_visit_offset_clause(self, _n: &OffsetClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `OFFSET` clause are visited.
    fn post_visit_offset_clause(self, _n: &OffsetClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SLIMIT` clause are visited.
    fn pre_visit_slimit_clause(self, _n: &SLimitClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SLIMIT` clause are visited.
    fn post_visit_slimit_clause(self, _n: &SLimitClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of the `SOFFSET` clause are visited.
    fn pre_visit_soffset_clause(self, _n: &SOffsetClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SOFFSET` clause are visited.
    fn post_visit_soffset_clause(self, _n: &SOffsetClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a `TZ` clause are visited.
    fn pre_visit_timezone_clause(
        self,
        _n: &TimeZoneClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `TZ` clause are visited.
    fn post_visit_timezone_clause(self, _n: &TimeZoneClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of an extended `ON` clause are visited.
    fn pre_visit_extended_on_clause(
        self,
        _n: &ExtendedOnClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of an extended `ON` clause are visited.
    fn post_visit_extended_on_clause(self, _n: &ExtendedOnClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of an `ON` clause are visited.
    fn pre_visit_on_clause(self, _n: &OnClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of an `ON` clause are visited.
    fn post_visit_on_clause(self, _n: &OnClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a `WITH MEASUREMENT` clause  are visited.
    fn pre_visit_with_measurement_clause(
        self,
        _n: &WithMeasurementClause,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `WITH MEASUREMENT` clause  are visited.
    fn post_visit_with_measurement_clause(
        self,
        _n: &WithMeasurementClause,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a `WITH KEY` clause are visited.
    fn pre_visit_with_key_clause(self, _n: &WithKeyClause) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `WITH KEY` clause  are visited.
    fn post_visit_with_key_clause(self, _n: &WithKeyClause) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a variable reference are visited.
    fn pre_visit_var_ref(self, _n: &VarRef) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a variable reference are visited.
    fn post_visit_var_ref(self, _n: &VarRef) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a function call are visited.
    fn pre_visit_call(self, _n: &Call) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a function call are visited.
    fn post_visit_call(self, _n: &Call) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a binary expression are visited.
    fn pre_visit_expr_binary(self, _n: &Binary) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a binary expression are visited.
    fn post_visit_expr_binary(self, _n: &Binary) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a conditional binary expression are visited.
    fn pre_visit_conditional_binary(
        self,
        _n: &ConditionalBinary,
    ) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a conditional binary expression are visited.
    fn post_visit_conditional_binary(self, _n: &ConditionalBinary) -> Result<Self, Self::Error> {
        Ok(self)
    }

    /// Invoked before any children of a literal are visited.
    fn pre_visit_literal(self, _n: &Literal) -> Result<Recursion<Self>, Self::Error> {
        Ok(Continue(self))
    }

    /// Invoked after a literal is visited.
    fn post_visit_literal(self, _n: &Literal) -> Result<Self, Self::Error> {
        Ok(self)
    }
}

/// Trait for types that can be visited by [`Visitor`]
pub trait Visitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error>;
}

impl Visitable for Statement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::CreateDatabase(s) => s.accept(visitor),
            Self::Delete(s) => s.accept(visitor),
            Self::DropMeasurement(s) => s.accept(visitor),
            Self::Explain(s) => s.accept(visitor),
            Self::Select(s) => s.accept(visitor),
            Self::ShowDatabases(s) => s.accept(visitor),
            Self::ShowMeasurements(s) => s.accept(visitor),
            Self::ShowRetentionPolicies(s) => s.accept(visitor),
            Self::ShowTagKeys(s) => s.accept(visitor),
            Self::ShowTagValues(s) => s.accept(visitor),
            Self::ShowFieldKeys(s) => s.accept(visitor),
        }?;

        visitor.post_visit_statement(self)
    }
}

impl Visitable for CreateDatabaseStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_create_database_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_create_database_statement(self)
    }
}

impl Visitable for DeleteStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_delete_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::FromWhere { from, condition } => {
                let visitor = from.accept(visitor)?;

                if let Some(condition) = condition {
                    condition.accept(visitor)
                } else {
                    Ok(visitor)
                }
            }
            Self::Where(condition) => condition.accept(visitor),
        }?;

        visitor.post_visit_delete_statement(self)
    }
}

impl Visitable for WhereClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_where_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.0.accept(visitor)?;

        visitor.post_visit_where_clause(self)
    }
}

impl Visitable for DeleteFromClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_delete_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, n| n.accept(v))?;

        visitor.post_visit_delete_from_clause(self)
    }
}

impl Visitable for MeasurementName {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_measurement_name(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_measurement_name(self)
    }
}

impl Visitable for DropMeasurementStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_drop_measurement_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_drop_measurement_statement(self)
    }
}

impl Visitable for ExplainStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_explain_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.select.accept(visitor)?;

        visitor.post_visit_explain_statement(self)
    }
}

impl Visitable for SelectStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.fields.accept(visitor)?;

        let visitor = self.from.accept(visitor)?;

        let visitor = if let Some(condition) = &self.condition {
            condition.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(group_by) = &self.group_by {
            group_by.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(fill_clause) = &self.fill {
            fill_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(order_by) = &self.order_by {
            order_by.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.series_limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.series_offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(tz_clause) = &self.timezone {
            tz_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_select_statement(self)
    }
}

impl Visitable for TimeZoneClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_timezone_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_timezone_clause(self)
    }
}

impl Visitable for LimitClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_limit_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_limit_clause(self)
    }
}

impl Visitable for OffsetClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_offset_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_offset_clause(self)
    }
}

impl Visitable for SLimitClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_slimit_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_slimit_clause(self)
    }
}

impl Visitable for SOffsetClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_soffset_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_soffset_clause(self)
    }
}

impl Visitable for FillClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_fill_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_fill_clause(self)
    }
}

impl Visitable for OrderByClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_order_by_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_order_by_clause(self)
    }
}

impl Visitable for GroupByClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_group_by_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, d| d.accept(v))?;

        visitor.post_visit_group_by_clause(self)
    }
}

impl Visitable for ShowMeasurementsStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_measurements_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = if let Some(on_clause) = &self.on {
            on_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(with_clause) = &self.with_measurement {
            with_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(condition) = &self.condition {
            condition.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_show_measurements_statement(self)
    }
}

impl Visitable for ExtendedOnClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_extended_on_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_extended_on_clause(self)
    }
}

impl Visitable for WithMeasurementClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_with_measurement_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Equals(n) => n.accept(visitor),
            Self::Regex(n) => n.accept(visitor),
        }?;

        visitor.post_visit_with_measurement_clause(self)
    }
}

impl Visitable for ShowRetentionPoliciesStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_retention_policies_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = if let Some(on_clause) = &self.database {
            on_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_show_retention_policies_statement(self)
    }
}

impl Visitable for ShowFromClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_show_from_clause(self)
    }
}

impl Visitable for QualifiedMeasurementName {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_qualified_measurement_name(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.name.accept(visitor)?;

        visitor.post_visit_qualified_measurement_name(self)
    }
}

impl Visitable for ShowTagKeysStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_tag_keys_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = if let Some(on_clause) = &self.database {
            on_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(from) = &self.from {
            from.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(condition) = &self.condition {
            condition.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_show_tag_keys_statement(self)
    }
}

impl Visitable for ShowTagValuesStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_tag_values_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = if let Some(on_clause) = &self.database {
            on_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(from) = &self.from {
            from.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = self.with_key.accept(visitor)?;

        let visitor = if let Some(condition) = &self.condition {
            condition.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_show_tag_values_statement(self)
    }
}

impl Visitable for ShowFieldKeysStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_field_keys_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = if let Some(on_clause) = &self.database {
            on_clause.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(from) = &self.from {
            from.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(limit) = &self.limit {
            limit.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)
        } else {
            Ok(visitor)
        }?;

        visitor.post_visit_show_field_keys_statement(self)
    }
}

impl Visitable for FieldList {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_field_list(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_select_field_list(self)
    }
}

impl Visitable for Field {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_field(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.expr.accept(visitor)?;

        visitor.post_visit_select_field(self)
    }
}

impl Visitable for FromMeasurementClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_select_from_clause(self)
    }
}

impl Visitable for MeasurementSelection {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_measurement_selection(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Name(name) => name.accept(visitor),
            Self::Subquery(select) => select.accept(visitor),
        }?;

        visitor.post_visit_select_measurement_selection(self)
    }
}

impl Visitable for Dimension {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_dimension(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Time(v) => v.accept(visitor),
            Self::VarRef(_) | Self::Regex(_) | Self::Wildcard => Ok(visitor),
        }?;

        visitor.post_visit_select_dimension(self)
    }
}

impl Visitable for TimeDimension {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_select_time_dimension(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.interval.accept(visitor)?;
        let visitor = if let Some(offset) = &self.offset {
            offset.accept(visitor)?
        } else {
            visitor
        };

        visitor.post_visit_select_time_dimension(self)
    }
}

impl Visitable for WithKeyClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_with_key_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_with_key_clause(self)
    }
}

impl Visitable for ShowDatabasesStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_show_databases_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };
        visitor.post_visit_show_databases_statement(self)
    }
}

impl Visitable for ConditionalExpression {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_conditional_expression(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Expr(expr) => expr.accept(visitor),
            Self::Binary(expr) => expr.accept(visitor),
            Self::Grouped(expr) => expr.accept(visitor),
        }?;

        visitor.post_visit_conditional_expression(self)
    }
}

impl Visitable for Expr {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_expr(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Call(expr) => expr.accept(visitor),
            Self::Binary(expr) => expr.accept(visitor),
            Self::Nested(expr) => expr.accept(visitor),
            Self::VarRef(expr) => expr.accept(visitor),
            Self::Literal(expr) => expr.accept(visitor),

            // We explicitly list out each enumeration, to ensure
            // we revisit if new items are added to the Expr enumeration.
            Self::BindParameter(_) | Self::Wildcard(_) | Self::Distinct(_) => Ok(visitor),
        }?;

        visitor.post_visit_expr(self)
    }
}

impl Visitable for Literal {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_literal(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_literal(self)
    }
}

impl Visitable for OnClause {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_on_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_on_clause(self)
    }
}

impl Visitable for VarRef {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_var_ref(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_var_ref(self)
    }
}

impl Visitable for Call {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_call(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.args.iter().try_fold(visitor, |v, n| n.accept(v))?;

        visitor.post_visit_call(self)
    }
}

impl Visitable for Binary {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_expr_binary(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.lhs.accept(visitor)?;
        let visitor = self.rhs.accept(visitor)?;

        visitor.post_visit_expr_binary(self)
    }
}

impl Visitable for ConditionalBinary {
    fn accept<V: Visitor>(&self, visitor: V) -> Result<V, V::Error> {
        let visitor = match visitor.pre_visit_conditional_binary(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.lhs.accept(visitor)?;
        let visitor = self.rhs.accept(visitor)?;

        visitor.post_visit_conditional_binary(self)
    }
}

#[cfg(test)]
mod test {
    use super::Recursion::Continue;
    use super::{Recursion, Visitable, Visitor};
    use crate::common::{
        LimitClause, MeasurementName, OffsetClause, OrderByClause, QualifiedMeasurementName,
        WhereClause,
    };
    use crate::delete::DeleteStatement;
    use crate::drop::DropMeasurementStatement;
    use crate::explain::ExplainStatement;
    use crate::expression::arithmetic::Expr;
    use crate::expression::conditional::ConditionalExpression;
    use crate::expression::{Binary, Call, ConditionalBinary, VarRef};
    use crate::literal::Literal;
    use crate::select::{
        Dimension, Field, FieldList, FillClause, FromMeasurementClause, GroupByClause,
        MeasurementSelection, SLimitClause, SOffsetClause, SelectStatement, TimeDimension,
        TimeZoneClause,
    };
    use crate::show::{OnClause, ShowDatabasesStatement};
    use crate::show_field_keys::ShowFieldKeysStatement;
    use crate::show_measurements::{
        ExtendedOnClause, ShowMeasurementsStatement, WithMeasurementClause,
    };
    use crate::show_retention_policies::ShowRetentionPoliciesStatement;
    use crate::show_tag_keys::ShowTagKeysStatement;
    use crate::show_tag_values::{ShowTagValuesStatement, WithKeyClause};
    use crate::simple_from_clause::{DeleteFromClause, ShowFromClause};
    use crate::statement::{statement, Statement};

    struct TestVisitor(Vec<String>);

    impl TestVisitor {
        fn new() -> Self {
            Self(Vec::new())
        }

        fn push_pre(self, name: &str) -> Self {
            let mut s = self.0;
            s.push(format!("pre_visit_{name}"));
            Self(s)
        }

        fn push_post(self, name: &str) -> Self {
            let mut s = self.0;
            s.push(format!("post_visit_{name}"));
            Self(s)
        }
    }

    macro_rules! trace_visit {
        ($NAME:ident, $TYPE:ty) => {
            paste::paste! {
                fn [<pre_visit_ $NAME>](self, _n: &$TYPE) -> Result<Recursion<Self>, Self::Error> {
                    Ok(Continue(self.push_pre(stringify!($NAME))))
                }

                fn [<post_visit_ $NAME>](self, _n: &$TYPE) -> Result<Self, Self::Error> {
                    Ok(self.push_post(stringify!($NAME)))
                }
            }
        };
    }

    impl Visitor for TestVisitor {
        type Error = ();

        trace_visit!(statement, Statement);
        trace_visit!(delete_statement, DeleteStatement);
        trace_visit!(delete_from_clause, DeleteFromClause);
        trace_visit!(measurement_name, MeasurementName);
        trace_visit!(drop_measurement_statement, DropMeasurementStatement);
        trace_visit!(explain_statement, ExplainStatement);
        trace_visit!(select_statement, SelectStatement);
        trace_visit!(show_databases_statement, ShowDatabasesStatement);
        trace_visit!(show_measurements_statement, ShowMeasurementsStatement);
        trace_visit!(
            show_retention_policies_statement,
            ShowRetentionPoliciesStatement
        );
        trace_visit!(show_tag_keys_statement, ShowTagKeysStatement);
        trace_visit!(show_tag_values_statement, ShowTagValuesStatement);
        trace_visit!(show_field_keys_statement, ShowFieldKeysStatement);
        trace_visit!(conditional_expression, ConditionalExpression);
        trace_visit!(expr, Expr);
        trace_visit!(select_field_list, FieldList);
        trace_visit!(select_field, Field);
        trace_visit!(select_from_clause, FromMeasurementClause);
        trace_visit!(select_measurement_selection, MeasurementSelection);
        trace_visit!(group_by_clause, GroupByClause);
        trace_visit!(select_dimension, Dimension);
        trace_visit!(select_time_dimension, TimeDimension);
        trace_visit!(where_clause, WhereClause);
        trace_visit!(show_from_clause, ShowFromClause);
        trace_visit!(qualified_measurement_name, QualifiedMeasurementName);
        trace_visit!(fill_clause, FillClause);
        trace_visit!(order_by_clause, OrderByClause);
        trace_visit!(limit_clause, LimitClause);
        trace_visit!(offset_clause, OffsetClause);
        trace_visit!(slimit_clause, SLimitClause);
        trace_visit!(soffset_clause, SOffsetClause);
        trace_visit!(timezone_clause, TimeZoneClause);
        trace_visit!(extended_on_clause, ExtendedOnClause);
        trace_visit!(on_clause, OnClause);
        trace_visit!(with_measurement_clause, WithMeasurementClause);
        trace_visit!(with_key_clause, WithKeyClause);
        trace_visit!(var_ref, VarRef);
        trace_visit!(call, Call);
        trace_visit!(expr_binary, Binary);
        trace_visit!(conditional_binary, ConditionalBinary);
        trace_visit!(literal, Literal);
    }

    macro_rules! visit_statement {
        ($SQL:literal) => {{
            let (_, s) = statement($SQL).unwrap();
            s.accept(TestVisitor::new()).unwrap().0
        }};
    }

    #[test]
    fn test_delete_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("DELETE FROM a WHERE b = \"c\""));
        insta::assert_yaml_snapshot!(visit_statement!("DELETE WHERE 'foo bar' =~ /foo/"));
        insta::assert_yaml_snapshot!(visit_statement!("DELETE FROM cpu"));
        insta::assert_yaml_snapshot!(visit_statement!("DELETE FROM /^cpu/"));
    }

    #[test]
    fn test_drop_measurement_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("DROP MEASUREMENT cpu"))
    }

    #[test]
    fn test_explain_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("EXPLAIN SELECT * FROM cpu"));
    }

    #[test]
    fn test_select_statement() {
        insta::assert_yaml_snapshot!(visit_statement!(r#"SELECT value FROM temp"#));
        insta::assert_yaml_snapshot!(visit_statement!(r#"SELECT DISTINCT value FROM temp"#));
        insta::assert_yaml_snapshot!(visit_statement!(r#"SELECT COUNT(value) FROM temp"#));
        insta::assert_yaml_snapshot!(visit_statement!(
            r#"SELECT COUNT(DISTINCT value) FROM temp"#
        ));
        insta::assert_yaml_snapshot!(visit_statement!(r#"SELECT * FROM /cpu/, memory"#));
        insta::assert_yaml_snapshot!(visit_statement!(
            r#"SELECT value FROM (SELECT usage FROM cpu WHERE host = "node1")
            WHERE region =~ /west/ AND value > 5
            GROUP BY TIME(5m), host
            FILL(previous)
            ORDER BY TIME DESC
            LIMIT 1 OFFSET 2
            SLIMIT 3 SOFFSET 4
            TZ('Australia/Hobart')
        "#
        ));
    }

    #[test]
    fn test_show_databases_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW DATABASES"));
    }

    #[test]
    fn test_show_measurements_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW MEASUREMENTS"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW MEASUREMENTS ON db.rp"));
        insta::assert_yaml_snapshot!(visit_statement!(
            "SHOW MEASUREMENTS WITH MEASUREMENT = \"cpu\""
        ));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW MEASUREMENTS WHERE host = 'west'"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW MEASUREMENTS LIMIT 5"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW MEASUREMENTS OFFSET 10"));

        insta::assert_yaml_snapshot!(visit_statement!(
            "SHOW MEASUREMENTS ON * WITH MEASUREMENT =~ /foo/ WHERE host = 'west' LIMIT 10 OFFSET 20"
        ));
    }

    #[test]
    fn test_show_retention_policies_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW RETENTION POLICIES"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW RETENTION POLICIES ON telegraf"));
    }

    #[test]
    fn test_show_tag_keys_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW TAG KEYS"));
        insta::assert_yaml_snapshot!(visit_statement!(
            "SHOW TAG KEYS ON telegraf FROM cpu WHERE host = \"west\" LIMIT 5 OFFSET 10"
        ));
    }

    #[test]
    fn test_show_tag_values_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW TAG VALUES WITH KEY = host"));
        insta::assert_yaml_snapshot!(visit_statement!(
            "SHOW TAG VALUES WITH KEY =~ /host|region/"
        ));
        insta::assert_yaml_snapshot!(visit_statement!(
            "SHOW TAG VALUES WITH KEY IN (host, region)"
        ));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW TAG VALUES ON telegraf FROM cpu WITH KEY = host WHERE host = \"west\" LIMIT 5 OFFSET 10"));
    }

    #[test]
    fn test_show_field_keys_statement() {
        insta::assert_yaml_snapshot!(visit_statement!("SHOW FIELD KEYS"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW FIELD KEYS ON telegraf"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW FIELD KEYS FROM cpu"));
        insta::assert_yaml_snapshot!(visit_statement!("SHOW FIELD KEYS ON telegraf FROM /cpu/"));
    }
}
