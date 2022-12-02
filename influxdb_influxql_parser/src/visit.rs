//! The visit module provides API for walking the AST.
//!
//! # Example
//!
//! ```
//! use influxdb_influxql_parser::visit::{Visitable, Visitor, VisitorResult};
//! use influxdb_influxql_parser::parse_statements;
//! use influxdb_influxql_parser::common::WhereClause;
//!
//! struct MyVisitor;
//!
//! impl Visitor for MyVisitor {
//!     fn post_visit_where_clause(self, n: &WhereClause) -> VisitorResult<Self> {
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
use crate::select::{
    Dimension, Field, FieldList, FillClause, FromMeasurementClause, GroupByClause,
    MeasurementSelection, SLimitClause, SOffsetClause, SelectStatement, TimeZoneClause,
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

/// The result type for a [`Visitor`].
pub type VisitorResult<T, E = &'static str> = Result<T, E>;

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
    /// Invoked before any children of the InfluxQL statement are visited.
    fn pre_visit_statement(self, _n: &Statement) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the InfluxQL statement are visited.
    fn post_visit_statement(self, _n: &Statement) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of `n` are visited.
    fn pre_visit_create_database_statement(
        self,
        _n: &CreateDatabaseStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of `n` are visited. Default
    /// implementation does nothing.
    fn post_visit_create_database_statement(
        self,
        _n: &CreateDatabaseStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `DELETE` statement are visited.
    fn pre_visit_delete_statement(self, _n: &DeleteStatement) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `DELETE` statement are visited.
    fn post_visit_delete_statement(self, _n: &DeleteStatement) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause of a `DELETE` statement are visited.
    fn pre_visit_delete_from_clause(self, _n: &DeleteFromClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause of a `DELETE` statement are visited.
    fn post_visit_delete_from_clause(self, _n: &DeleteFromClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the measurement name are visited.
    fn pre_visit_measurement_name(self, _n: &MeasurementName) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the measurement name are visited.
    fn post_visit_measurement_name(self, _n: &MeasurementName) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `DROP MEASUREMENT` statement are visited.
    fn pre_visit_drop_measurement_statement(
        self,
        _n: &DropMeasurementStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `DROP MEASUREMENT` statement are visited.
    fn post_visit_drop_measurement_statement(
        self,
        _n: &DropMeasurementStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `EXPLAIN` statement are visited.
    fn pre_visit_explain_statement(self, _n: &ExplainStatement) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `EXPLAIN` statement are visited.
    fn post_visit_explain_statement(self, _n: &ExplainStatement) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SELECT` statement are visited.
    fn pre_visit_select_statement(self, _n: &SelectStatement) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SELECT` statement are visited.
    fn post_visit_select_statement(self, _n: &SelectStatement) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW DATABASES` statement are visited.
    fn pre_visit_show_databases_statement(
        self,
        _n: &ShowDatabasesStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW DATABASES` statement are visited.
    fn post_visit_show_databases_statement(
        self,
        _n: &ShowDatabasesStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW MEASUREMENTS` statement are visited.
    fn pre_visit_show_measurements_statement(
        self,
        _n: &ShowMeasurementsStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW MEASUREMENTS` statement are visited.
    fn post_visit_show_measurements_statement(
        self,
        _n: &ShowMeasurementsStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW RETENTION POLICIES` statement are visited.
    fn pre_visit_show_retention_policies_statement(
        self,
        _n: &ShowRetentionPoliciesStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW RETENTION POLICIES` statement are visited.
    fn post_visit_show_retention_policies_statement(
        self,
        _n: &ShowRetentionPoliciesStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW TAG KEYS` statement are visited.
    fn pre_visit_show_tag_keys_statement(
        self,
        _n: &ShowTagKeysStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW TAG KEYS` statement are visited.
    fn post_visit_show_tag_keys_statement(self, _n: &ShowTagKeysStatement) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW TAG VALUES` statement are visited.
    fn pre_visit_show_tag_values_statement(
        self,
        _n: &ShowTagValuesStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW TAG VALUES` statement are visited.
    fn post_visit_show_tag_values_statement(
        self,
        _n: &ShowTagValuesStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SHOW FIELD KEYS` statement are visited.
    fn pre_visit_show_field_keys_statement(
        self,
        _n: &ShowFieldKeysStatement,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SHOW FIELD KEYS` statement are visited.
    fn post_visit_show_field_keys_statement(
        self,
        _n: &ShowFieldKeysStatement,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the conditional expression are visited.
    fn pre_visit_conditional_expression(
        self,
        _n: &ConditionalExpression,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the conditional expression are visited.
    fn post_visit_conditional_expression(self, _n: &ConditionalExpression) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the arithmetic expression are visited.
    fn pre_visit_expr(self, _n: &Expr) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the arithmetic expression are visited.
    fn post_visit_expr(self, _n: &Expr) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any fields of the `SELECT` projection are visited.
    fn pre_visit_select_field_list(self, _n: &FieldList) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all fields of the `SELECT` projection are visited.
    fn post_visit_select_field_list(self, _n: &FieldList) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the field of a `SELECT` statement are visited.
    fn pre_visit_select_field(self, _n: &Field) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the field of a `SELECT` statement are visited.
    fn post_visit_select_field(self, _n: &Field) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause of a `SELECT` statement are visited.
    fn pre_visit_select_from_clause(
        self,
        _n: &FromMeasurementClause,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause of a `SELECT` statement are visited.
    fn post_visit_select_from_clause(self, _n: &FromMeasurementClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn pre_visit_select_measurement_selection(
        self,
        _n: &MeasurementSelection,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn post_visit_select_measurement_selection(
        self,
        _n: &MeasurementSelection,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `GROUP BY` clause are visited.
    fn pre_visit_group_by_clause(self, _n: &GroupByClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `GROUP BY` clause are visited.
    fn post_visit_group_by_clause(self, _n: &GroupByClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `GROUP BY` dimension expression are visited.
    fn pre_visit_select_dimension(self, _n: &Dimension) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `GROUP BY` dimension expression are visited.
    fn post_visit_select_dimension(self, _n: &Dimension) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `WHERE` clause are visited.
    fn pre_visit_where_clause(self, _n: &WhereClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `WHERE` clause are visited.
    fn post_visit_where_clause(self, _n: &WhereClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `FROM` clause for any `SHOW` statement are visited.
    fn pre_visit_show_from_clause(self, _n: &ShowFromClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FROM` clause for any `SHOW` statement are visited.
    fn post_visit_show_from_clause(self, _n: &ShowFromClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the qualified measurement name are visited.
    fn pre_visit_qualified_measurement_name(
        self,
        _n: &QualifiedMeasurementName,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the qualified measurement name are visited.
    fn post_visit_qualified_measurement_name(
        self,
        _n: &QualifiedMeasurementName,
    ) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `FILL` clause are visited.
    fn pre_visit_fill_clause(self, _n: &FillClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `FILL` clause are visited.
    fn post_visit_fill_clause(self, _n: &FillClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `ORDER BY` clause are visited.
    fn pre_visit_order_by_clause(self, _n: &OrderByClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `ORDER BY` clause are visited.
    fn post_visit_order_by_clause(self, _n: &OrderByClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `LIMIT` clause are visited.
    fn pre_visit_limit_clause(self, _n: &LimitClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `LIMIT` clause are visited.
    fn post_visit_limit_clause(self, _n: &LimitClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `OFFSET` clause are visited.
    fn pre_visit_offset_clause(self, _n: &OffsetClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `OFFSET` clause are visited.
    fn post_visit_offset_clause(self, _n: &OffsetClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SLIMIT` clause are visited.
    fn pre_visit_slimit_clause(self, _n: &SLimitClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SLIMIT` clause are visited.
    fn post_visit_slimit_clause(self, _n: &SLimitClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of the `SOFFSET` clause are visited.
    fn pre_visit_soffset_clause(self, _n: &SOffsetClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of the `SOFFSET` clause are visited.
    fn post_visit_soffset_clause(self, _n: &SOffsetClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of a `TZ` clause are visited.
    fn pre_visit_timezone_clause(self, _n: &TimeZoneClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `TZ` clause are visited.
    fn post_visit_timezone_clause(self, _n: &TimeZoneClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of an extended `ON` clause are visited.
    fn pre_visit_extended_on_clause(self, _n: &ExtendedOnClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of an extended `ON` clause are visited.
    fn post_visit_extended_on_clause(self, _n: &ExtendedOnClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of an `ON` clause are visited.
    fn pre_visit_on_clause(self, _n: &OnClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of an `ON` clause are visited.
    fn post_visit_on_clause(self, _n: &OnClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of a `WITH MEASUREMENT` clause  are visited.
    fn pre_visit_with_measurement_clause(
        self,
        _n: &WithMeasurementClause,
    ) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `WITH MEASUREMENT` clause  are visited.
    fn post_visit_with_measurement_clause(self, _n: &WithMeasurementClause) -> VisitorResult<Self> {
        Ok(self)
    }

    /// Invoked before any children of a `WITH KEY` clause are visited.
    fn pre_visit_with_key_clause(self, _n: &WithKeyClause) -> VisitorResult<Recursion<Self>> {
        Ok(Continue(self))
    }

    /// Invoked after all children of a `WITH KEY` clause  are visited.
    fn post_visit_with_key_clause(self, _n: &WithKeyClause) -> VisitorResult<Self> {
        Ok(self)
    }
}

/// Trait for types that can be visited by [`Visitor`]
pub trait Visitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V>;
}

impl Visitable for Statement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_create_database_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_create_database_statement(self)
    }
}

impl Visitable for DeleteStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_where_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.0.accept(visitor)?;

        visitor.post_visit_where_clause(self)
    }
}

impl Visitable for DeleteFromClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_delete_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, n| n.accept(v))?;

        visitor.post_visit_delete_from_clause(self)
    }
}

impl Visitable for MeasurementName {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_measurement_name(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_measurement_name(self)
    }
}

impl Visitable for DropMeasurementStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_drop_measurement_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_drop_measurement_statement(self)
    }
}

impl Visitable for ExplainStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_explain_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.select.accept(visitor)?;

        visitor.post_visit_explain_statement(self)
    }
}

impl Visitable for SelectStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_timezone_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_timezone_clause(self)
    }
}

impl Visitable for LimitClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_limit_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_limit_clause(self)
    }
}

impl Visitable for OffsetClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_offset_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_offset_clause(self)
    }
}

impl Visitable for SLimitClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_slimit_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_slimit_clause(self)
    }
}

impl Visitable for SOffsetClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_soffset_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_soffset_clause(self)
    }
}

impl Visitable for FillClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_fill_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_fill_clause(self)
    }
}

impl Visitable for OrderByClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_order_by_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_order_by_clause(self)
    }
}

impl Visitable for GroupByClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_group_by_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, d| d.accept(v))?;

        visitor.post_visit_group_by_clause(self)
    }
}

impl Visitable for ShowMeasurementsStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_extended_on_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_extended_on_clause(self)
    }
}

impl Visitable for WithMeasurementClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_show_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_show_from_clause(self)
    }
}

impl Visitable for QualifiedMeasurementName {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_qualified_measurement_name(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.name.accept(visitor)?;

        visitor.post_visit_qualified_measurement_name(self)
    }
}

impl Visitable for ShowTagKeysStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_select_field_list(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_select_field_list(self)
    }
}

impl Visitable for Field {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_select_field(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.expr.accept(visitor)?;

        visitor.post_visit_select_field(self)
    }
}

impl Visitable for FromMeasurementClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_select_from_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = self.contents.iter().try_fold(visitor, |v, f| f.accept(v))?;

        visitor.post_visit_select_from_clause(self)
    }
}

impl Visitable for MeasurementSelection {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
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
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_select_dimension(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Time { interval, offset } => {
                let visitor = interval.accept(visitor)?;
                if let Some(offset) = offset {
                    offset.accept(visitor)
                } else {
                    Ok(visitor)
                }
            }
            Self::Tag(_) | Self::Regex(_) | Self::Wildcard => Ok(visitor),
        }?;

        visitor.post_visit_select_dimension(self)
    }
}

impl Visitable for WithKeyClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_with_key_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_with_key_clause(self)
    }
}

impl Visitable for ShowDatabasesStatement {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_show_databases_statement(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };
        visitor.post_visit_show_databases_statement(self)
    }
}

impl Visitable for ConditionalExpression {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_conditional_expression(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::Expr(expr) => expr.accept(visitor),
            Self::Binary { lhs, rhs, .. } => {
                let visitor = lhs.accept(visitor)?;
                rhs.accept(visitor)
            }
            Self::Grouped(expr) => expr.accept(visitor),
        }?;

        visitor.post_visit_conditional_expression(self)
    }
}

impl Visitable for Expr {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_expr(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        let visitor = match self {
            Self::UnaryOp(_, expr) => expr.accept(visitor),
            Self::Call { args, .. } => args.iter().try_fold(visitor, |v, e| e.accept(v)),
            Self::Binary { lhs, op: _, rhs } => {
                let visitor = lhs.accept(visitor)?;
                rhs.accept(visitor)
            }
            Self::Nested(expr) => expr.accept(visitor),

            // We explicitly list out each enumeration, to ensure
            // we revisit if new items are added to the Expr enumeration.
            Self::VarRef { .. }
            | Self::BindParameter(_)
            | Self::Literal(_)
            | Self::Wildcard(_)
            | Self::Distinct(_) => Ok(visitor),
        }?;

        visitor.post_visit_expr(self)
    }
}

impl Visitable for OnClause {
    fn accept<V: Visitor>(&self, visitor: V) -> VisitorResult<V> {
        let visitor = match visitor.pre_visit_on_clause(self)? {
            Continue(visitor) => visitor,
            Stop(visitor) => return Ok(visitor),
        };

        visitor.post_visit_on_clause(self)
    }
}

#[cfg(test)]
mod test {
    use super::Recursion::Continue;
    use super::{Recursion, Visitable, Visitor, VisitorResult};
    use crate::common::{
        LimitClause, MeasurementName, OffsetClause, OrderByClause, QualifiedMeasurementName,
        WhereClause,
    };
    use crate::delete::DeleteStatement;
    use crate::drop::DropMeasurementStatement;
    use crate::explain::ExplainStatement;
    use crate::expression::arithmetic::Expr;
    use crate::expression::conditional::ConditionalExpression;
    use crate::select::{
        Dimension, Field, FieldList, FillClause, FromMeasurementClause, GroupByClause,
        MeasurementSelection, SLimitClause, SOffsetClause, SelectStatement, TimeZoneClause,
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
    use std::fmt::Debug;

    struct TestVisitor(Vec<String>);

    impl TestVisitor {
        fn new() -> Self {
            Self(Vec::new())
        }

        fn push_pre(self, name: &str, n: impl Debug) -> Self {
            let mut s = self.0;
            s.push(format!("pre_visit_{}: {:?}", name, n));
            Self(s)
        }

        fn push_post(self, name: &str, n: impl Debug) -> Self {
            let mut s = self.0;
            s.push(format!("post_visit_{}: {:?}", name, n));
            Self(s)
        }
    }

    impl Visitor for TestVisitor {
        fn pre_visit_statement(self, n: &Statement) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("statement", n)))
        }

        fn post_visit_statement(self, n: &Statement) -> VisitorResult<Self> {
            Ok(self.push_post("statement", n))
        }

        fn pre_visit_delete_statement(self, n: &DeleteStatement) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("delete_statement", n)))
        }

        fn post_visit_delete_statement(self, n: &DeleteStatement) -> VisitorResult<Self> {
            Ok(self.push_post("delete_statement", n))
        }

        fn pre_visit_delete_from_clause(
            self,
            n: &DeleteFromClause,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("delete_from", n)))
        }

        fn post_visit_delete_from_clause(self, n: &DeleteFromClause) -> VisitorResult<Self> {
            Ok(self.push_post("delete_from", n))
        }

        fn pre_visit_measurement_name(self, n: &MeasurementName) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("measurement_name", n)))
        }

        fn post_visit_measurement_name(self, n: &MeasurementName) -> VisitorResult<Self> {
            Ok(self.push_post("measurement_name", n))
        }

        fn pre_visit_drop_measurement_statement(
            self,
            n: &DropMeasurementStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("drop_measurement_statement", n)))
        }

        fn post_visit_drop_measurement_statement(
            self,
            n: &DropMeasurementStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("drop_measurement_statement", n))
        }

        fn pre_visit_explain_statement(
            self,
            n: &ExplainStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("explain_statement", n)))
        }

        fn post_visit_explain_statement(self, n: &ExplainStatement) -> VisitorResult<Self> {
            Ok(self.push_post("explain_statement", n))
        }

        fn pre_visit_select_statement(self, n: &SelectStatement) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_statement", n)))
        }

        fn post_visit_select_statement(self, n: &SelectStatement) -> VisitorResult<Self> {
            Ok(self.push_post("select_statement", n))
        }

        fn pre_visit_show_databases_statement(
            self,
            n: &ShowDatabasesStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_databases_statement", n)))
        }

        fn post_visit_show_databases_statement(
            self,
            n: &ShowDatabasesStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_databases_statement", n))
        }

        fn pre_visit_show_measurements_statement(
            self,
            n: &ShowMeasurementsStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_measurements_statement", n)))
        }

        fn post_visit_show_measurements_statement(
            self,
            n: &ShowMeasurementsStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_measurements_statement", n))
        }

        fn pre_visit_show_retention_policies_statement(
            self,
            n: &ShowRetentionPoliciesStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(
                self.push_pre("show_retention_policies_statement", n),
            ))
        }

        fn post_visit_show_retention_policies_statement(
            self,
            n: &ShowRetentionPoliciesStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_retention_policies_statement", n))
        }

        fn pre_visit_show_tag_keys_statement(
            self,
            n: &ShowTagKeysStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_tag_keys_statement", n)))
        }

        fn post_visit_show_tag_keys_statement(
            self,
            n: &ShowTagKeysStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_tag_keys_statement", n))
        }

        fn pre_visit_show_tag_values_statement(
            self,
            n: &ShowTagValuesStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_tag_values_statement", n)))
        }

        fn post_visit_show_tag_values_statement(
            self,
            n: &ShowTagValuesStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_tag_values_statement", n))
        }

        fn pre_visit_show_field_keys_statement(
            self,
            n: &ShowFieldKeysStatement,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_field_keys_statement", n)))
        }

        fn post_visit_show_field_keys_statement(
            self,
            n: &ShowFieldKeysStatement,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("show_field_keys_statement", n))
        }

        fn pre_visit_conditional_expression(
            self,
            n: &ConditionalExpression,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("conditional_expression", n)))
        }

        fn post_visit_conditional_expression(
            self,
            n: &ConditionalExpression,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("conditional_expression", n))
        }

        fn pre_visit_expr(self, n: &Expr) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("expr", n)))
        }

        fn post_visit_expr(self, n: &Expr) -> VisitorResult<Self> {
            Ok(self.push_post("expr", n))
        }

        fn pre_visit_select_field_list(self, n: &FieldList) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_field_list", n)))
        }

        fn post_visit_select_field_list(self, n: &FieldList) -> VisitorResult<Self> {
            Ok(self.push_post("select_field_list", n))
        }

        fn pre_visit_select_field(self, n: &Field) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_field", n)))
        }

        fn post_visit_select_field(self, n: &Field) -> VisitorResult<Self> {
            Ok(self.push_post("select_field", n))
        }

        fn pre_visit_select_from_clause(
            self,
            n: &FromMeasurementClause,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_from_clause", n)))
        }

        fn post_visit_select_from_clause(self, n: &FromMeasurementClause) -> VisitorResult<Self> {
            Ok(self.push_post("select_from_clause", n))
        }

        fn pre_visit_select_measurement_selection(
            self,
            n: &MeasurementSelection,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_measurement_selection", n)))
        }

        fn post_visit_select_measurement_selection(
            self,
            n: &MeasurementSelection,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("select_measurement_selection", n))
        }

        fn pre_visit_group_by_clause(self, n: &GroupByClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("group_by_clause", n)))
        }

        fn post_visit_group_by_clause(self, n: &GroupByClause) -> VisitorResult<Self> {
            Ok(self.push_post("group_by_clause", n))
        }

        fn pre_visit_select_dimension(self, n: &Dimension) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("select_dimension", n)))
        }

        fn post_visit_select_dimension(self, n: &Dimension) -> VisitorResult<Self> {
            Ok(self.push_post("select_dimension", n))
        }

        fn pre_visit_where_clause(self, n: &WhereClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("where_clause", n)))
        }

        fn post_visit_where_clause(self, n: &WhereClause) -> VisitorResult<Self> {
            Ok(self.push_post("where_clause", n))
        }

        fn pre_visit_show_from_clause(self, n: &ShowFromClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("show_from_clause", n)))
        }

        fn post_visit_show_from_clause(self, n: &ShowFromClause) -> VisitorResult<Self> {
            Ok(self.push_post("show_from_clause", n))
        }

        fn pre_visit_qualified_measurement_name(
            self,
            n: &QualifiedMeasurementName,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("qualified_measurement_name", n)))
        }

        fn post_visit_qualified_measurement_name(
            self,
            n: &QualifiedMeasurementName,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("qualified_measurement_name", n))
        }

        fn pre_visit_fill_clause(self, n: &FillClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("fill_clause", n)))
        }

        fn post_visit_fill_clause(self, n: &FillClause) -> VisitorResult<Self> {
            Ok(self.push_post("fill_clause", n))
        }

        fn pre_visit_order_by_clause(self, n: &OrderByClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("order_by_clause", n)))
        }

        fn post_visit_order_by_clause(self, n: &OrderByClause) -> VisitorResult<Self> {
            Ok(self.push_post("order_by_clause", n))
        }

        fn pre_visit_limit_clause(self, n: &LimitClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("limit_clause", n)))
        }

        fn post_visit_limit_clause(self, n: &LimitClause) -> VisitorResult<Self> {
            Ok(self.push_post("limit_clause", n))
        }

        fn pre_visit_offset_clause(self, n: &OffsetClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("offset_clause", n)))
        }

        fn post_visit_offset_clause(self, n: &OffsetClause) -> VisitorResult<Self> {
            Ok(self.push_post("offset_clause", n))
        }

        fn pre_visit_slimit_clause(self, n: &SLimitClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("slimit_clause", n)))
        }

        fn post_visit_slimit_clause(self, n: &SLimitClause) -> VisitorResult<Self> {
            Ok(self.push_post("slimit_clause", n))
        }

        fn pre_visit_soffset_clause(self, n: &SOffsetClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("soffset_clause", n)))
        }

        fn post_visit_soffset_clause(self, n: &SOffsetClause) -> VisitorResult<Self> {
            Ok(self.push_post("soffset_clause", n))
        }

        fn pre_visit_timezone_clause(self, n: &TimeZoneClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("timezone_clause", n)))
        }

        fn post_visit_timezone_clause(self, n: &TimeZoneClause) -> VisitorResult<Self> {
            Ok(self.push_post("timezone_clause", n))
        }

        fn pre_visit_extended_on_clause(
            self,
            n: &ExtendedOnClause,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("extended_on_clause", n)))
        }

        fn post_visit_extended_on_clause(self, n: &ExtendedOnClause) -> VisitorResult<Self> {
            Ok(self.push_post("extended_on_clause", n))
        }

        fn pre_visit_on_clause(self, n: &OnClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("on_clause", n)))
        }

        fn post_visit_on_clause(self, n: &OnClause) -> VisitorResult<Self> {
            Ok(self.push_pre("on_clause", n))
        }

        fn pre_visit_with_measurement_clause(
            self,
            n: &WithMeasurementClause,
        ) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("with_measurement_clause", n)))
        }

        fn post_visit_with_measurement_clause(
            self,
            n: &WithMeasurementClause,
        ) -> VisitorResult<Self> {
            Ok(self.push_post("with_measurement_clause", n))
        }

        fn pre_visit_with_key_clause(self, n: &WithKeyClause) -> VisitorResult<Recursion<Self>> {
            Ok(Continue(self.push_pre("with_key_clause", n)))
        }

        fn post_visit_with_key_clause(self, n: &WithKeyClause) -> VisitorResult<Self> {
            Ok(self.push_post("with_key_clause", n))
        }
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
