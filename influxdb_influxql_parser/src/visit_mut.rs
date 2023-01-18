//! The visit module provides API for walking the AST.
//!
//! # Example
//!
//! ```
//! use influxdb_influxql_parser::visit_mut::{VisitableMut, VisitorMut};
//! use influxdb_influxql_parser::parse_statements;
//! use influxdb_influxql_parser::common::WhereClause;
//!
//! struct MyVisitor;
//!
//! impl VisitorMut for MyVisitor {
//!     type Error = ();
//!
//!     fn post_visit_where_clause(&mut self, n: &mut WhereClause) -> Result<(), Self::Error> {
//!         println!("{}", n);
//!         Ok(())
//!     }
//! }
//!
//! let statements = parse_statements("SELECT value FROM cpu WHERE host = 'west'").unwrap();
//! let mut statement  = statements.first().unwrap().clone();
//! let mut vis = MyVisitor;
//! statement.accept(&mut vis).unwrap();
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

/// Controls how the visitor recursion should proceed.
#[derive(Clone, Copy)]
pub enum Recursion {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue,
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop,
}

/// Encode the depth-first traversal of an InfluxQL statement. When passed to
/// any [`VisitableMut::accept`], `pre_visit` functions are invoked repeatedly
/// until a leaf node is reached or a `pre_visit` function returns [`Recursion::Stop`].
pub trait VisitorMut: Sized {
    /// The type returned in the event of an error traversing the tree.
    type Error;

    /// Invoked before any children of the InfluxQL statement are visited.
    fn pre_visit_statement(&mut self, _n: &mut Statement) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the InfluxQL statement are visited.
    fn post_visit_statement(&mut self, _n: &mut Statement) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of `n` are visited.
    fn pre_visit_create_database_statement(
        &mut self,
        _n: &mut CreateDatabaseStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of `n` are visited. Default
    /// implementation does nothing.
    fn post_visit_create_database_statement(
        &mut self,
        _n: &mut CreateDatabaseStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `DELETE` statement are visited.
    fn pre_visit_delete_statement(
        &mut self,
        _n: &mut DeleteStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `DELETE` statement are visited.
    fn post_visit_delete_statement(&mut self, _n: &mut DeleteStatement) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `FROM` clause of a `DELETE` statement are visited.
    fn pre_visit_delete_from_clause(
        &mut self,
        _n: &mut DeleteFromClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `FROM` clause of a `DELETE` statement are visited.
    fn post_visit_delete_from_clause(
        &mut self,
        _n: &mut DeleteFromClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the measurement name are visited.
    fn pre_visit_measurement_name(
        &mut self,
        _n: &mut MeasurementName,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the measurement name are visited.
    fn post_visit_measurement_name(&mut self, _n: &mut MeasurementName) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `DROP MEASUREMENT` statement are visited.
    fn pre_visit_drop_measurement_statement(
        &mut self,
        _n: &mut DropMeasurementStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `DROP MEASUREMENT` statement are visited.
    fn post_visit_drop_measurement_statement(
        &mut self,
        _n: &mut DropMeasurementStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `EXPLAIN` statement are visited.
    fn pre_visit_explain_statement(
        &mut self,
        _n: &mut ExplainStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `EXPLAIN` statement are visited.
    fn post_visit_explain_statement(
        &mut self,
        _n: &mut ExplainStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SELECT` statement are visited.
    fn pre_visit_select_statement(
        &mut self,
        _n: &mut SelectStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SELECT` statement are visited.
    fn post_visit_select_statement(&mut self, _n: &mut SelectStatement) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW DATABASES` statement are visited.
    fn pre_visit_show_databases_statement(
        &mut self,
        _n: &mut ShowDatabasesStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW DATABASES` statement are visited.
    fn post_visit_show_databases_statement(
        &mut self,
        _n: &mut ShowDatabasesStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW MEASUREMENTS` statement are visited.
    fn pre_visit_show_measurements_statement(
        &mut self,
        _n: &mut ShowMeasurementsStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW MEASUREMENTS` statement are visited.
    fn post_visit_show_measurements_statement(
        &mut self,
        _n: &mut ShowMeasurementsStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW RETENTION POLICIES` statement are visited.
    fn pre_visit_show_retention_policies_statement(
        &mut self,
        _n: &mut ShowRetentionPoliciesStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW RETENTION POLICIES` statement are visited.
    fn post_visit_show_retention_policies_statement(
        &mut self,
        _n: &mut ShowRetentionPoliciesStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW TAG KEYS` statement are visited.
    fn pre_visit_show_tag_keys_statement(
        &mut self,
        _n: &mut ShowTagKeysStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW TAG KEYS` statement are visited.
    fn post_visit_show_tag_keys_statement(
        &mut self,
        _n: &mut ShowTagKeysStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW TAG VALUES` statement are visited.
    fn pre_visit_show_tag_values_statement(
        &mut self,
        _n: &mut ShowTagValuesStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW TAG VALUES` statement are visited.
    fn post_visit_show_tag_values_statement(
        &mut self,
        _n: &mut ShowTagValuesStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SHOW FIELD KEYS` statement are visited.
    fn pre_visit_show_field_keys_statement(
        &mut self,
        _n: &mut ShowFieldKeysStatement,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SHOW FIELD KEYS` statement are visited.
    fn post_visit_show_field_keys_statement(
        &mut self,
        _n: &mut ShowFieldKeysStatement,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the conditional expression are visited.
    fn pre_visit_conditional_expression(
        &mut self,
        _n: &mut ConditionalExpression,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the conditional expression are visited.
    fn post_visit_conditional_expression(
        &mut self,
        _n: &mut ConditionalExpression,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the arithmetic expression are visited.
    fn pre_visit_expr(&mut self, _n: &mut Expr) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the arithmetic expression are visited.
    fn post_visit_expr(&mut self, _n: &mut Expr) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any fields of the `SELECT` projection are visited.
    fn pre_visit_select_field_list(
        &mut self,
        _n: &mut FieldList,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all fields of the `SELECT` projection are visited.
    fn post_visit_select_field_list(&mut self, _n: &mut FieldList) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the field of a `SELECT` statement are visited.
    fn pre_visit_select_field(&mut self, _n: &mut Field) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the field of a `SELECT` statement are visited.
    fn post_visit_select_field(&mut self, _n: &mut Field) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `FROM` clause of a `SELECT` statement are visited.
    fn pre_visit_select_from_clause(
        &mut self,
        _n: &mut FromMeasurementClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `FROM` clause of a `SELECT` statement are visited.
    fn post_visit_select_from_clause(
        &mut self,
        _n: &mut FromMeasurementClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn pre_visit_select_measurement_selection(
        &mut self,
        _n: &mut MeasurementSelection,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the measurement selection of a `FROM` clause for a `SELECT` statement are visited.
    fn post_visit_select_measurement_selection(
        &mut self,
        _n: &mut MeasurementSelection,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `GROUP BY` clause are visited.
    fn pre_visit_group_by_clause(
        &mut self,
        _n: &mut GroupByClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `GROUP BY` clause are visited.
    fn post_visit_group_by_clause(&mut self, _n: &mut GroupByClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `GROUP BY` dimension expression are visited.
    fn pre_visit_select_dimension(&mut self, _n: &mut Dimension) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `GROUP BY` dimension expression are visited.
    fn post_visit_select_dimension(&mut self, _n: &mut Dimension) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `WHERE` clause are visited.
    fn pre_visit_where_clause(&mut self, _n: &mut WhereClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `WHERE` clause are visited.
    fn post_visit_where_clause(&mut self, _n: &mut WhereClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `FROM` clause for any `SHOW` statement are visited.
    fn pre_visit_show_from_clause(
        &mut self,
        _n: &mut ShowFromClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `FROM` clause for any `SHOW` statement are visited.
    fn post_visit_show_from_clause(&mut self, _n: &mut ShowFromClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the qualified measurement name are visited.
    fn pre_visit_qualified_measurement_name(
        &mut self,
        _n: &mut QualifiedMeasurementName,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the qualified measurement name are visited.
    fn post_visit_qualified_measurement_name(
        &mut self,
        _n: &mut QualifiedMeasurementName,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `FILL` clause are visited.
    fn pre_visit_fill_clause(&mut self, _n: &mut FillClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `FILL` clause are visited.
    fn post_visit_fill_clause(&mut self, _n: &mut FillClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `ORDER BY` clause are visited.
    fn pre_visit_order_by_clause(
        &mut self,
        _n: &mut OrderByClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `ORDER BY` clause are visited.
    fn post_visit_order_by_clause(&mut self, _n: &mut OrderByClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `LIMIT` clause are visited.
    fn pre_visit_limit_clause(&mut self, _n: &mut LimitClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `LIMIT` clause are visited.
    fn post_visit_limit_clause(&mut self, _n: &mut LimitClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `OFFSET` clause are visited.
    fn pre_visit_offset_clause(&mut self, _n: &mut OffsetClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `OFFSET` clause are visited.
    fn post_visit_offset_clause(&mut self, _n: &mut OffsetClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SLIMIT` clause are visited.
    fn pre_visit_slimit_clause(&mut self, _n: &mut SLimitClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SLIMIT` clause are visited.
    fn post_visit_slimit_clause(&mut self, _n: &mut SLimitClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of the `SOFFSET` clause are visited.
    fn pre_visit_soffset_clause(
        &mut self,
        _n: &mut SOffsetClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of the `SOFFSET` clause are visited.
    fn post_visit_soffset_clause(&mut self, _n: &mut SOffsetClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of a `TZ` clause are visited.
    fn pre_visit_timezone_clause(
        &mut self,
        _n: &mut TimeZoneClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of a `TZ` clause are visited.
    fn post_visit_timezone_clause(&mut self, _n: &mut TimeZoneClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of an extended `ON` clause are visited.
    fn pre_visit_extended_on_clause(
        &mut self,
        _n: &mut ExtendedOnClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of an extended `ON` clause are visited.
    fn post_visit_extended_on_clause(
        &mut self,
        _n: &mut ExtendedOnClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of an `ON` clause are visited.
    fn pre_visit_on_clause(&mut self, _n: &mut OnClause) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of an `ON` clause are visited.
    fn post_visit_on_clause(&mut self, _n: &mut OnClause) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of a `WITH MEASUREMENT` clause  are visited.
    fn pre_visit_with_measurement_clause(
        &mut self,
        _n: &mut WithMeasurementClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of a `WITH MEASUREMENT` clause  are visited.
    fn post_visit_with_measurement_clause(
        &mut self,
        _n: &mut WithMeasurementClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked before any children of a `WITH KEY` clause are visited.
    fn pre_visit_with_key_clause(
        &mut self,
        _n: &mut WithKeyClause,
    ) -> Result<Recursion, Self::Error> {
        Ok(Continue)
    }

    /// Invoked after all children of a `WITH KEY` clause  are visited.
    fn post_visit_with_key_clause(&mut self, _n: &mut WithKeyClause) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Trait for types that can be visited by [`VisitorMut`]
pub trait VisitableMut: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error>;
}

impl VisitableMut for Statement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_statement(self)? {
            return Ok(());
        };

        match self {
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

impl VisitableMut for CreateDatabaseStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_create_database_statement(self)? {
            return Ok(());
        };

        visitor.post_visit_create_database_statement(self)
    }
}

impl VisitableMut for DeleteStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_delete_statement(self)? {
            return Ok(());
        };

        match self {
            Self::FromWhere { from, condition } => {
                from.accept(visitor)?;

                if let Some(condition) = condition {
                    condition.accept(visitor)?;
                }
            }
            Self::Where(condition) => condition.accept(visitor)?,
        };

        visitor.post_visit_delete_statement(self)
    }
}

impl VisitableMut for WhereClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_where_clause(self)? {
            return Ok(());
        };

        self.0.accept(visitor)?;

        visitor.post_visit_where_clause(self)
    }
}

impl VisitableMut for DeleteFromClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_delete_from_clause(self)? {
            return Ok(());
        };

        self.contents
            .iter_mut()
            .try_for_each(|n| n.accept(visitor))?;

        visitor.post_visit_delete_from_clause(self)
    }
}

impl VisitableMut for MeasurementName {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_measurement_name(self)? {
            return Ok(());
        };

        visitor.post_visit_measurement_name(self)
    }
}

impl VisitableMut for DropMeasurementStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_drop_measurement_statement(self)? {
            return Ok(());
        };

        visitor.post_visit_drop_measurement_statement(self)
    }
}

impl VisitableMut for ExplainStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_explain_statement(self)? {
            return Ok(());
        };

        self.select.accept(visitor)?;

        visitor.post_visit_explain_statement(self)
    }
}

impl VisitableMut for SelectStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_statement(self)? {
            return Ok(());
        };

        self.fields.accept(visitor)?;

        self.from.accept(visitor)?;

        if let Some(condition) = &mut self.condition {
            condition.accept(visitor)?;
        }

        if let Some(group_by) = &mut self.group_by {
            group_by.accept(visitor)?;
        }

        if let Some(fill_clause) = &mut self.fill {
            fill_clause.accept(visitor)?;
        }

        if let Some(order_by) = &mut self.order_by {
            order_by.accept(visitor)?;
        }

        if let Some(limit) = &mut self.limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.offset {
            offset.accept(visitor)?;
        }

        if let Some(limit) = &mut self.series_limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.series_offset {
            offset.accept(visitor)?;
        }

        if let Some(tz_clause) = &mut self.timezone {
            tz_clause.accept(visitor)?;
        }

        visitor.post_visit_select_statement(self)
    }
}

impl VisitableMut for TimeZoneClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_timezone_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_timezone_clause(self)
    }
}

impl VisitableMut for LimitClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_limit_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_limit_clause(self)
    }
}

impl VisitableMut for OffsetClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_offset_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_offset_clause(self)
    }
}

impl VisitableMut for SLimitClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_slimit_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_slimit_clause(self)
    }
}

impl VisitableMut for SOffsetClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_soffset_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_soffset_clause(self)
    }
}

impl VisitableMut for FillClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_fill_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_fill_clause(self)
    }
}

impl VisitableMut for OrderByClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_order_by_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_order_by_clause(self)
    }
}

impl VisitableMut for GroupByClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_group_by_clause(self)? {
            return Ok(());
        };

        self.contents
            .iter_mut()
            .try_for_each(|d| d.accept(visitor))?;

        visitor.post_visit_group_by_clause(self)
    }
}

impl VisitableMut for ShowMeasurementsStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_measurements_statement(self)? {
            return Ok(());
        };

        if let Some(on_clause) = &mut self.on {
            on_clause.accept(visitor)?;
        }

        if let Some(with_clause) = &mut self.with_measurement {
            with_clause.accept(visitor)?;
        }

        if let Some(condition) = &mut self.condition {
            condition.accept(visitor)?;
        }

        if let Some(limit) = &mut self.limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.offset {
            offset.accept(visitor)?;
        }

        visitor.post_visit_show_measurements_statement(self)
    }
}

impl VisitableMut for ExtendedOnClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_extended_on_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_extended_on_clause(self)
    }
}

impl VisitableMut for WithMeasurementClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_with_measurement_clause(self)? {
            return Ok(());
        };

        match self {
            Self::Equals(n) => n.accept(visitor),
            Self::Regex(n) => n.accept(visitor),
        }?;

        visitor.post_visit_with_measurement_clause(self)
    }
}

impl VisitableMut for ShowRetentionPoliciesStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_retention_policies_statement(self)? {
            return Ok(());
        };

        if let Some(on_clause) = &mut self.database {
            on_clause.accept(visitor)?;
        }

        visitor.post_visit_show_retention_policies_statement(self)
    }
}

impl VisitableMut for ShowFromClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_from_clause(self)? {
            return Ok(());
        };

        self.contents
            .iter_mut()
            .try_for_each(|f| f.accept(visitor))?;

        visitor.post_visit_show_from_clause(self)
    }
}

impl VisitableMut for QualifiedMeasurementName {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_qualified_measurement_name(self)? {
            return Ok(());
        };

        self.name.accept(visitor)?;

        visitor.post_visit_qualified_measurement_name(self)
    }
}

impl VisitableMut for ShowTagKeysStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_tag_keys_statement(self)? {
            return Ok(());
        };

        if let Some(on_clause) = &mut self.database {
            on_clause.accept(visitor)?;
        }

        if let Some(from) = &mut self.from {
            from.accept(visitor)?;
        }

        if let Some(condition) = &mut self.condition {
            condition.accept(visitor)?;
        }

        if let Some(limit) = &mut self.limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.offset {
            offset.accept(visitor)?;
        }

        visitor.post_visit_show_tag_keys_statement(self)
    }
}

impl VisitableMut for ShowTagValuesStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_tag_values_statement(self)? {
            return Ok(());
        };

        if let Some(on_clause) = &mut self.database {
            on_clause.accept(visitor)?;
        }

        if let Some(from) = &mut self.from {
            from.accept(visitor)?;
        }

        self.with_key.accept(visitor)?;

        if let Some(condition) = &mut self.condition {
            condition.accept(visitor)?;
        }

        if let Some(limit) = &mut self.limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.offset {
            offset.accept(visitor)?;
        }

        visitor.post_visit_show_tag_values_statement(self)
    }
}

impl VisitableMut for ShowFieldKeysStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_field_keys_statement(self)? {
            return Ok(());
        };

        if let Some(on_clause) = &mut self.database {
            on_clause.accept(visitor)?;
        }

        if let Some(from) = &mut self.from {
            from.accept(visitor)?;
        }

        if let Some(limit) = &mut self.limit {
            limit.accept(visitor)?;
        }

        if let Some(offset) = &mut self.offset {
            offset.accept(visitor)?;
        }

        visitor.post_visit_show_field_keys_statement(self)
    }
}

impl VisitableMut for FieldList {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_field_list(self)? {
            return Ok(());
        };

        self.contents
            .iter_mut()
            .try_for_each(|f| f.accept(visitor))?;

        visitor.post_visit_select_field_list(self)
    }
}

impl VisitableMut for Field {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_field(self)? {
            return Ok(());
        };

        self.expr.accept(visitor)?;

        visitor.post_visit_select_field(self)
    }
}

impl VisitableMut for FromMeasurementClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_from_clause(self)? {
            return Ok(());
        };

        self.contents
            .iter_mut()
            .try_for_each(|f| f.accept(visitor))?;

        visitor.post_visit_select_from_clause(self)
    }
}

impl VisitableMut for MeasurementSelection {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_measurement_selection(self)? {
            return Ok(());
        };

        match self {
            Self::Name(name) => name.accept(visitor),
            Self::Subquery(select) => select.accept(visitor),
        }?;

        visitor.post_visit_select_measurement_selection(self)
    }
}

impl VisitableMut for Dimension {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_select_dimension(self)? {
            return Ok(());
        };

        match self {
            Self::Time { interval, offset } => {
                interval.accept(visitor)?;
                if let Some(offset) = offset {
                    offset.accept(visitor)?;
                }
            }
            Self::Tag(_) | Self::Regex(_) | Self::Wildcard => {}
        };

        visitor.post_visit_select_dimension(self)
    }
}

impl VisitableMut for WithKeyClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_with_key_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_with_key_clause(self)
    }
}

impl VisitableMut for ShowDatabasesStatement {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_show_databases_statement(self)? {
            return Ok(());
        };
        visitor.post_visit_show_databases_statement(self)
    }
}

impl VisitableMut for ConditionalExpression {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_conditional_expression(self)? {
            return Ok(());
        };

        match self {
            Self::Expr(expr) => expr.accept(visitor),
            Self::Binary { lhs, rhs, .. } => {
                lhs.accept(visitor)?;
                rhs.accept(visitor)
            }
            Self::Grouped(expr) => expr.accept(visitor),
        }?;

        visitor.post_visit_conditional_expression(self)
    }
}

impl VisitableMut for Expr {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_expr(self)? {
            return Ok(());
        };

        match self {
            Self::Call { args, .. } => args.iter_mut().try_for_each(|e| e.accept(visitor))?,
            Self::Binary { lhs, op: _, rhs } => {
                lhs.accept(visitor)?;
                rhs.accept(visitor)?;
            }
            Self::Nested(expr) => expr.accept(visitor)?,

            // We explicitly list out each enumeration, to ensure
            // we revisit if new items are added to the Expr enumeration.
            Self::VarRef { .. }
            | Self::BindParameter(_)
            | Self::Literal(_)
            | Self::Wildcard(_)
            | Self::Distinct(_) => {}
        };

        visitor.post_visit_expr(self)
    }
}

impl VisitableMut for OnClause {
    fn accept<V: VisitorMut>(&mut self, visitor: &mut V) -> Result<(), V::Error> {
        if let Stop = visitor.pre_visit_on_clause(self)? {
            return Ok(());
        };

        visitor.post_visit_on_clause(self)
    }
}

#[cfg(test)]
mod test {
    use super::Recursion::Continue;
    use super::{Recursion, VisitableMut, VisitorMut};
    use crate::common::{
        LimitClause, MeasurementName, OffsetClause, OrderByClause, QualifiedMeasurementName,
        WhereClause,
    };
    use crate::delete::DeleteStatement;
    use crate::drop::DropMeasurementStatement;
    use crate::explain::ExplainStatement;
    use crate::expression::arithmetic::Expr;
    use crate::expression::conditional::ConditionalExpression;
    use crate::parse_statements;
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

        fn push_pre(&mut self, name: &str, n: impl Debug) {
            self.0.push(format!("pre_visit_{}: {:?}", name, n));
        }

        fn push_post(&mut self, name: &str, n: impl Debug) {
            self.0.push(format!("post_visit_{}: {:?}", name, n));
        }
    }

    impl VisitorMut for TestVisitor {
        type Error = ();

        fn pre_visit_statement(&mut self, n: &mut Statement) -> Result<Recursion, Self::Error> {
            self.push_pre("statement", n);
            Ok(Continue)
        }

        fn post_visit_statement(&mut self, n: &mut Statement) -> Result<(), Self::Error> {
            self.push_post("statement", n);
            Ok(())
        }

        fn pre_visit_delete_statement(
            &mut self,
            n: &mut DeleteStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("delete_statement", n);
            Ok(Continue)
        }

        fn post_visit_delete_statement(
            &mut self,
            n: &mut DeleteStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("delete_statement", n);
            Ok(())
        }

        fn pre_visit_delete_from_clause(
            &mut self,
            n: &mut DeleteFromClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("delete_from", n);
            Ok(Continue)
        }

        fn post_visit_delete_from_clause(
            &mut self,
            n: &mut DeleteFromClause,
        ) -> Result<(), Self::Error> {
            self.push_post("delete_from", n);
            Ok(())
        }

        fn pre_visit_measurement_name(
            &mut self,
            n: &mut MeasurementName,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("measurement_name", n);
            Ok(Continue)
        }

        fn post_visit_measurement_name(
            &mut self,
            n: &mut MeasurementName,
        ) -> Result<(), Self::Error> {
            self.push_post("measurement_name", n);
            Ok(())
        }

        fn pre_visit_drop_measurement_statement(
            &mut self,
            n: &mut DropMeasurementStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("drop_measurement_statement", n);
            Ok(Continue)
        }

        fn post_visit_drop_measurement_statement(
            &mut self,
            n: &mut DropMeasurementStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("drop_measurement_statement", n);
            Ok(())
        }

        fn pre_visit_explain_statement(
            &mut self,
            n: &mut ExplainStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("explain_statement", n);
            Ok(Continue)
        }

        fn post_visit_explain_statement(
            &mut self,
            n: &mut ExplainStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("explain_statement", n);
            Ok(())
        }

        fn pre_visit_select_statement(
            &mut self,
            n: &mut SelectStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("select_statement", n);
            Ok(Continue)
        }

        fn post_visit_select_statement(
            &mut self,
            n: &mut SelectStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("select_statement", n);
            Ok(())
        }

        fn pre_visit_show_databases_statement(
            &mut self,
            n: &mut ShowDatabasesStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_databases_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_databases_statement(
            &mut self,
            n: &mut ShowDatabasesStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_databases_statement", n);
            Ok(())
        }

        fn pre_visit_show_measurements_statement(
            &mut self,
            n: &mut ShowMeasurementsStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_measurements_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_measurements_statement(
            &mut self,
            n: &mut ShowMeasurementsStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_measurements_statement", n);
            Ok(())
        }

        fn pre_visit_show_retention_policies_statement(
            &mut self,
            n: &mut ShowRetentionPoliciesStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_retention_policies_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_retention_policies_statement(
            &mut self,
            n: &mut ShowRetentionPoliciesStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_retention_policies_statement", n);
            Ok(())
        }

        fn pre_visit_show_tag_keys_statement(
            &mut self,
            n: &mut ShowTagKeysStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_tag_keys_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_tag_keys_statement(
            &mut self,
            n: &mut ShowTagKeysStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_tag_keys_statement", n);
            Ok(())
        }

        fn pre_visit_show_tag_values_statement(
            &mut self,
            n: &mut ShowTagValuesStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_tag_values_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_tag_values_statement(
            &mut self,
            n: &mut ShowTagValuesStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_tag_values_statement", n);
            Ok(())
        }

        fn pre_visit_show_field_keys_statement(
            &mut self,
            n: &mut ShowFieldKeysStatement,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_field_keys_statement", n);
            Ok(Continue)
        }

        fn post_visit_show_field_keys_statement(
            &mut self,
            n: &mut ShowFieldKeysStatement,
        ) -> Result<(), Self::Error> {
            self.push_post("show_field_keys_statement", n);
            Ok(())
        }

        fn pre_visit_conditional_expression(
            &mut self,
            n: &mut ConditionalExpression,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("conditional_expression", n);
            Ok(Continue)
        }

        fn post_visit_conditional_expression(
            &mut self,
            n: &mut ConditionalExpression,
        ) -> Result<(), Self::Error> {
            self.push_post("conditional_expression", n);
            Ok(())
        }

        fn pre_visit_expr(&mut self, n: &mut Expr) -> Result<Recursion, Self::Error> {
            self.push_pre("expr", n);
            Ok(Continue)
        }

        fn post_visit_expr(&mut self, n: &mut Expr) -> Result<(), Self::Error> {
            self.push_post("expr", n);
            Ok(())
        }

        fn pre_visit_select_field_list(
            &mut self,
            n: &mut FieldList,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("select_field_list", n);
            Ok(Continue)
        }

        fn post_visit_select_field_list(&mut self, n: &mut FieldList) -> Result<(), Self::Error> {
            self.push_post("select_field_list", n);
            Ok(())
        }

        fn pre_visit_select_field(&mut self, n: &mut Field) -> Result<Recursion, Self::Error> {
            self.push_pre("select_field", n);
            Ok(Continue)
        }

        fn post_visit_select_field(&mut self, n: &mut Field) -> Result<(), Self::Error> {
            self.push_post("select_field", n);
            Ok(())
        }

        fn pre_visit_select_from_clause(
            &mut self,
            n: &mut FromMeasurementClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("select_from_clause", n);
            Ok(Continue)
        }

        fn post_visit_select_from_clause(
            &mut self,
            n: &mut FromMeasurementClause,
        ) -> Result<(), Self::Error> {
            self.push_post("select_from_clause", n);
            Ok(())
        }

        fn pre_visit_select_measurement_selection(
            &mut self,
            n: &mut MeasurementSelection,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("select_measurement_selection", n);
            Ok(Continue)
        }

        fn post_visit_select_measurement_selection(
            &mut self,
            n: &mut MeasurementSelection,
        ) -> Result<(), Self::Error> {
            self.push_post("select_measurement_selection", n);
            Ok(())
        }

        fn pre_visit_group_by_clause(
            &mut self,
            n: &mut GroupByClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("group_by_clause", n);
            Ok(Continue)
        }

        fn post_visit_group_by_clause(&mut self, n: &mut GroupByClause) -> Result<(), Self::Error> {
            self.push_post("group_by_clause", n);
            Ok(())
        }

        fn pre_visit_select_dimension(
            &mut self,
            n: &mut Dimension,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("select_dimension", n);
            Ok(Continue)
        }

        fn post_visit_select_dimension(&mut self, n: &mut Dimension) -> Result<(), Self::Error> {
            self.push_post("select_dimension", n);
            Ok(())
        }

        fn pre_visit_where_clause(
            &mut self,
            n: &mut WhereClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("where_clause", n);
            Ok(Continue)
        }

        fn post_visit_where_clause(&mut self, n: &mut WhereClause) -> Result<(), Self::Error> {
            self.push_post("where_clause", n);
            Ok(())
        }

        fn pre_visit_show_from_clause(
            &mut self,
            n: &mut ShowFromClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("show_from_clause", n);
            Ok(Continue)
        }

        fn post_visit_show_from_clause(
            &mut self,
            n: &mut ShowFromClause,
        ) -> Result<(), Self::Error> {
            self.push_post("show_from_clause", n);
            Ok(())
        }

        fn pre_visit_qualified_measurement_name(
            &mut self,
            n: &mut QualifiedMeasurementName,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("qualified_measurement_name", n);
            Ok(Continue)
        }

        fn post_visit_qualified_measurement_name(
            &mut self,
            n: &mut QualifiedMeasurementName,
        ) -> Result<(), Self::Error> {
            self.push_post("qualified_measurement_name", n);
            Ok(())
        }

        fn pre_visit_fill_clause(&mut self, n: &mut FillClause) -> Result<Recursion, Self::Error> {
            self.push_pre("fill_clause", n);
            Ok(Continue)
        }

        fn post_visit_fill_clause(&mut self, n: &mut FillClause) -> Result<(), Self::Error> {
            self.push_post("fill_clause", n);
            Ok(())
        }

        fn pre_visit_order_by_clause(
            &mut self,
            n: &mut OrderByClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("order_by_clause", n);
            Ok(Continue)
        }

        fn post_visit_order_by_clause(&mut self, n: &mut OrderByClause) -> Result<(), Self::Error> {
            self.push_post("order_by_clause", n);
            Ok(())
        }

        fn pre_visit_limit_clause(
            &mut self,
            n: &mut LimitClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("limit_clause", n);
            Ok(Continue)
        }

        fn post_visit_limit_clause(&mut self, n: &mut LimitClause) -> Result<(), Self::Error> {
            self.push_post("limit_clause", n);
            Ok(())
        }

        fn pre_visit_offset_clause(
            &mut self,
            n: &mut OffsetClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("offset_clause", n);
            Ok(Continue)
        }

        fn post_visit_offset_clause(&mut self, n: &mut OffsetClause) -> Result<(), Self::Error> {
            self.push_post("offset_clause", n);
            Ok(())
        }

        fn pre_visit_slimit_clause(
            &mut self,
            n: &mut SLimitClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("slimit_clause", n);
            Ok(Continue)
        }

        fn post_visit_slimit_clause(&mut self, n: &mut SLimitClause) -> Result<(), Self::Error> {
            self.push_post("slimit_clause", n);
            Ok(())
        }

        fn pre_visit_soffset_clause(
            &mut self,
            n: &mut SOffsetClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("soffset_clause", n);
            Ok(Continue)
        }

        fn post_visit_soffset_clause(&mut self, n: &mut SOffsetClause) -> Result<(), Self::Error> {
            self.push_post("soffset_clause", n);
            Ok(())
        }

        fn pre_visit_timezone_clause(
            &mut self,
            n: &mut TimeZoneClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("timezone_clause", n);
            Ok(Continue)
        }

        fn post_visit_timezone_clause(
            &mut self,
            n: &mut TimeZoneClause,
        ) -> Result<(), Self::Error> {
            self.push_post("timezone_clause", n);
            Ok(())
        }

        fn pre_visit_extended_on_clause(
            &mut self,
            n: &mut ExtendedOnClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("extended_on_clause", n);
            Ok(Continue)
        }

        fn post_visit_extended_on_clause(
            &mut self,
            n: &mut ExtendedOnClause,
        ) -> Result<(), Self::Error> {
            self.push_post("extended_on_clause", n);
            Ok(())
        }

        fn pre_visit_on_clause(&mut self, n: &mut OnClause) -> Result<Recursion, Self::Error> {
            self.push_pre("on_clause", n);
            Ok(Continue)
        }

        fn post_visit_on_clause(&mut self, n: &mut OnClause) -> Result<(), Self::Error> {
            self.push_pre("on_clause", n);
            Ok(())
        }

        fn pre_visit_with_measurement_clause(
            &mut self,
            n: &mut WithMeasurementClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("with_measurement_clause", n);
            Ok(Continue)
        }

        fn post_visit_with_measurement_clause(
            &mut self,
            n: &mut WithMeasurementClause,
        ) -> Result<(), Self::Error> {
            self.push_post("with_measurement_clause", n);
            Ok(())
        }

        fn pre_visit_with_key_clause(
            &mut self,
            n: &mut WithKeyClause,
        ) -> Result<Recursion, Self::Error> {
            self.push_pre("with_key_clause", n);
            Ok(Continue)
        }

        fn post_visit_with_key_clause(&mut self, n: &mut WithKeyClause) -> Result<(), Self::Error> {
            self.push_post("with_key_clause", n);
            Ok(())
        }
    }

    macro_rules! visit_statement {
        ($SQL:literal) => {{
            let (_, mut s) = statement($SQL).unwrap();
            let mut vis = TestVisitor::new();
            s.accept(&mut vis).unwrap();
            vis.0
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

    #[test]
    fn test_mutability() {
        struct AddLimit;

        impl VisitorMut for AddLimit {
            type Error = ();

            fn pre_visit_select_statement(
                &mut self,
                n: &mut SelectStatement,
            ) -> Result<Recursion, Self::Error> {
                n.limit = Some(LimitClause(10));
                Ok(Continue)
            }
        }

        let mut statement = parse_statements("SELECT usage FROM cpu")
            .unwrap()
            .first()
            .unwrap()
            .clone();
        let mut vis = AddLimit;
        statement.accept(&mut vis).unwrap();
        assert_eq!(statement.to_string(), "SELECT usage FROM cpu LIMIT 10");
    }
}
