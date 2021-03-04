use std::collections::{BTreeSet, HashSet};

use crate::dictionary::{Dictionary, Error as DictionaryError};

use arrow_deps::datafusion::{
    error::{DataFusionError, Result as DatafusionResult},
    logical_plan::{Expr, ExpressionVisitor, Operator, Recursion},
    optimizer::utils::expr_to_column_names,
};
use data_types::TIME_COLUMN_NAME;
use query::{
    predicate::TimestampRange,
    util::{make_range_expr, AndExprBuilder},
};
//use snafu::{OptionExt, ResultExt, Snafu};
use snafu::{ensure, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing table '{}': {}", table_name, source))]
    TableWrite {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Time Column was not not found in dictionary: {}", source))]
    TimeColumnNotFound { source: DictionaryError },

    #[snafu(display("Unsupported predicate. Mutable buffer does not support: {}", source))]
    UnsupportedPredicate { source: DataFusionError },

    #[snafu(display(
        "Internal error visiting expressions in ChunkPredicateBuilder: {}",
        source
    ))]
    InternalVisitingExpressions { source: DataFusionError },

    #[snafu(display("table_names has already been specified in ChunkPredicateBuilder"))]
    TableNamesAlreadySet {},

    #[snafu(display("field_names has already been specified in ChunkPredicateBuilder"))]
    FieldNamesAlreadySet {},

    #[snafu(display("range has already been specified in ChunkPredicateBuilder"))]
    RangeAlreadySet {},

    #[snafu(display("exprs has already been specified in ChunkPredicateBuilder"))]
    ExprsAlreadySet {},

    #[snafu(display("required_columns has already been specified in ChunkPredicateBuilder"))]
    RequiredColumnsAlreadySet {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Describes the result of translating a set of strings into
/// chunk specific ids
#[derive(Debug, PartialEq, Eq)]
pub enum ChunkIdSet {
    /// At least one of the strings was not present in the chunks'
    /// dictionary.
    ///
    /// This is important when testing for the presence of all ids in
    /// a set, as we know they can not all be present
    AtLeastOneMissing,

    /// All strings existed in this chunk's dictionary
    Present(BTreeSet<u32>),
}

/// a 'Compiled' set of predicates / filters that can be evaluated on
/// this chunk (where strings have been translated to chunk
/// specific u32 ids)
#[derive(Debug, Default)]
pub struct ChunkPredicate {
    /// If present, restrict the request to just those tables whose
    /// names are in table_names. If present but empty, means there
    /// was a predicate but no tables named that way exist in the
    /// chunk (so no table can pass)
    pub table_name_predicate: Option<BTreeSet<u32>>,

    /// Optional column restriction. If present, further
    /// restrict any field columns returned to only those named, and
    /// skip tables entirely when querying metadata that do not have
    /// *any* of the fields
    pub field_name_predicate: Option<BTreeSet<u32>>,

    /// General DataFusion expressions (arbitrary predicates) applied
    /// as a filter using logical conjuction (aka are 'AND'ed
    /// together). Only rows that evaluate to TRUE for all these
    /// expressions should be returned.
    ///
    /// TODO these exprs should eventually be removed (when they are
    /// all handled one layer up in the query layer)
    pub chunk_exprs: Vec<Expr>,

    /// If Some, then the table must contain all columns specified
    /// to pass the predicate
    pub required_columns: Option<ChunkIdSet>,

    /// The id of the "time" column in this chunk
    pub time_column_id: u32,

    /// Timestamp range: only rows within this range should be considered
    pub range: Option<TimestampRange>,
}

impl ChunkPredicate {
    /// Creates and adds a datafuson predicate representing the
    /// combination of predicate and timestamp.
    pub fn filter_expr(&self) -> Option<Expr> {
        // build up a list of expressions
        let mut builder =
            AndExprBuilder::default().append_opt(self.make_timestamp_predicate_expr());

        for expr in &self.chunk_exprs {
            builder = builder.append_expr(expr.clone());
        }

        builder.build()
    }

    /// For plans which select a subset of fields, returns true if
    /// the field should be included in the results
    pub fn should_include_field(&self, field_id: u32) -> bool {
        match &self.field_name_predicate {
            None => true,
            Some(field_restriction) => field_restriction.contains(&field_id),
        }
    }

    /// Return true if this column is the time column
    pub fn is_time_column(&self, id: u32) -> bool {
        self.time_column_id == id
    }

    /// Creates a DataFusion predicate for appliying a timestamp range:
    ///
    /// range.start <= time and time < range.end`
    fn make_timestamp_predicate_expr(&self) -> Option<Expr> {
        self.range
            .map(|range| make_range_expr(range.start, range.end))
    }
}

/// Builds ChunkPredicates
#[derive(Debug)]
pub struct ChunkPredicateBuilder<'a> {
    inner: ChunkPredicate,
    dictionary: &'a Dictionary,
}

impl<'a> ChunkPredicateBuilder<'a> {
    pub fn new(dictionary: &'a Dictionary) -> Result<Self> {
        let time_column_id = dictionary
            .lookup_value(TIME_COLUMN_NAME)
            .context(TimeColumnNotFound)?;

        let inner = ChunkPredicate {
            time_column_id,
            ..Default::default()
        };

        Ok(Self { inner, dictionary })
    }

    /// Set table_name_predicate so only tables in `names` are returned
    pub fn table_names(mut self, names: Option<&BTreeSet<String>>) -> Result<Self> {
        ensure!(
            self.inner.table_name_predicate.is_none(),
            TableNamesAlreadySet
        );
        self.inner.table_name_predicate = self.compile_string_list(names);
        Ok(self)
    }

    /// Set field_name_predicate so only tables in `names` are returned
    pub fn field_names(mut self, names: Option<&BTreeSet<String>>) -> Result<Self> {
        ensure!(
            self.inner.field_name_predicate.is_none(),
            FieldNamesAlreadySet
        );
        self.inner.field_name_predicate = self.compile_string_list(names);
        Ok(self)
    }

    pub fn range(mut self, range: Option<TimestampRange>) -> Result<Self> {
        ensure!(self.inner.range.is_none(), RangeAlreadySet);
        self.inner.range = range;
        Ok(self)
    }

    /// Set the general purpose predicates
    pub fn exprs(mut self, chunk_exprs: Vec<Expr>) -> Result<Self> {
        // In order to evaluate expressions in the table, all columns
        // referenced in the expression must appear (I think, not sure
        // about NOT, etc so panic if we see one of those);
        let mut visitor = SupportVisitor {};
        let mut predicate_columns: HashSet<String> = HashSet::new();
        for expr in &chunk_exprs {
            visitor = expr.accept(visitor).context(UnsupportedPredicate)?;
            expr_to_column_names(&expr, &mut predicate_columns)
                .context(InternalVisitingExpressions)?;
        }

        ensure!(self.inner.chunk_exprs.is_empty(), ExprsAlreadySet);
        self.inner.chunk_exprs = chunk_exprs;

        // if there are any column references in the expression, ensure they appear in
        // any table
        if !predicate_columns.is_empty() {
            ensure!(
                self.inner.required_columns.is_none(),
                RequiredColumnsAlreadySet
            );
            self.inner.required_columns = Some(self.make_chunk_ids(predicate_columns.iter()));
        }
        Ok(self)
    }

    /// Return the created chunk predicate, consuming self
    pub fn build(self) -> ChunkPredicate {
        self.inner
    }

    /// Converts a Set of strings into a set of ids in terms of this
    /// Chunk's dictionary.
    ///
    /// If there are no matching Strings in the chunks dictionary,
    /// those strings are ignored and a (potentially empty) set is
    /// returned.
    fn compile_string_list(&self, names: Option<&BTreeSet<String>>) -> Option<BTreeSet<u32>> {
        names.map(|names| {
            names
                .iter()
                .filter_map(|name| self.dictionary.id(name))
                .collect::<BTreeSet<_>>()
        })
    }

    /// Translate a bunch of strings into a set of ids from the dictionarythis
    /// chunk
    pub fn make_chunk_ids<'b, I>(&self, predicate_columns: I) -> ChunkIdSet
    where
        I: Iterator<Item = &'b String>,
    {
        let mut symbols = BTreeSet::new();
        for column_name in predicate_columns {
            if let Some(column_id) = self.dictionary.id(column_name) {
                symbols.insert(column_id);
            } else {
                return ChunkIdSet::AtLeastOneMissing;
            }
        }

        ChunkIdSet::Present(symbols)
    }
}

/// Used to figure out if we know how to deal with this kind of
/// predicate in the write buffer
struct SupportVisitor {}

impl ExpressionVisitor for SupportVisitor {
    fn pre_visit(self, expr: &Expr) -> DatafusionResult<Recursion<Self>> {
        match expr {
            Expr::Literal(..) => Ok(Recursion::Continue(self)),
            Expr::Column(..) => Ok(Recursion::Continue(self)),
            Expr::BinaryExpr { op, .. } => {
                match op {
                    Operator::Eq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::And
                    | Operator::Or => Ok(Recursion::Continue(self)),
                    // Unsupported (need to think about ramifications)
                    Operator::NotEq | Operator::Modulus | Operator::Like | Operator::NotLike => {
                        Err(DataFusionError::NotImplemented(format!(
                            "Operator {:?} not yet supported in IOx MutableBuffer",
                            op
                        )))
                    }
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported expression in mutable_buffer database: {:?}",
                expr
            ))),
        }
    }
}
