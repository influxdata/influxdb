//! Query builder for the native InfluxDB Flight API with support for parameters.

use std::{collections::HashMap, marker::PhantomData};

use generated_types::influxdata::iox::querier::v1::{
    read_info::{QueryParam, QueryType},
    ReadInfo,
};
use iox_query_params::StatementParam;

use super::{Client, IOxRecordBatchStream};

/// Initial type state for [`QueryBuilder`] when no query has been given via
/// the [`QueryBuilder::sql`] or [`QueryBuilder::influxql`] methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NoQuery;

/// Type state for [`QueryBuilder`] when a query has been given via the
/// [`QueryBuilder::sql`] or [`QueryBuilder::influxql`] methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Query;

/// Query builder for InfluxDB queries executed over the Flight protocol. Supports SQL and InfluxQL languages
/// optionally with named parameters.
///
/// See [`super::Client::query`] for information on how to create a QueryBuilder.
///
/// Use [`QueryBuilder::sql`] or [`QueryBuilder::influxql`] methods to supply the text of the query
/// as a string. Optionally, named parameters can be given with [`QueryBuilder::with_param`].
///
/// Finally, call [`QueryBuilder::run`] to execute the query.
///
/// The generic parameter `State` is a typestate indicating whether the query builder
/// has been initialized yet. This ensures that the builder cannot call [`QueryBuilder::run`] on a
/// query until all mandatory parameters are given.
#[derive(Debug)]
pub struct QueryBuilder<'client, State> {
    client: &'client mut Client,
    database: String,
    query_text: Option<String>,
    query_type: Option<QueryType>,
    params: HashMap<String, StatementParam>,
    _phantom: PhantomData<State>,
}

impl<'c, State> QueryBuilder<'c, State> {
    /// Supply a new named parameter to use with this query. Parameters referenced using
    /// `$placeholder` syntax in the query will be substituted with the value provided.
    ///
    /// Any type that can be converted to [`StatementParam`] can be used as a value. Here are some
    /// examples:
    ///
    /// ```rust, no_run
    ///     # use influxdb_iox_client::flight::query::{QueryBuilder, Query};
    ///     # let mut query: QueryBuilder<Query> = todo!();
    ///     query
    ///         .with_param("my_param1", true) // boolean
    ///         .with_param("my_param2", "Hello, World!") // string
    ///         .with_param("my_param3", 1234) // integer
    ///         .with_param("my_param4", 1.23) // floating point
    ///         .with_param("my_param5", Some("string")) // Option types can be converted
    ///         .with_param("my_param6", None::<Option<()>>) // Option types convert None to NULL
    ///     # ;
    /// ```
    ///
    pub fn with_param(mut self, name: impl Into<String>, value: impl Into<StatementParam>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    /// IMPORTANT NOTE: Named parameters currently do not work with this client until
    /// an upgrade to DataFusion 34.0 is performed. See <https://github.com/apache/arrow-datafusion/issues/8245>
    ///
    /// Supply an iterator of (name, value) pairs to use as named parameters for this query. Parameters referenced using
    /// `$placeholder` syntax in the query will be substituted with the values provided.
    ///
    /// This is equivalent to calling [`Self::with_param`) on each (name, value) pair of the
    /// provided iterator. See docs of that method for more details.
    pub fn with_params<N, V>(mut self, params: impl IntoIterator<Item = (N, V)>) -> Self
    where
        N: Into<String>,
        V: Into<StatementParam>,
    {
        self.params
            .extend(params.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

impl<'c> QueryBuilder<'c, NoQuery> {
    /// internal constructor. use [`super::Client::query`] as the public constructor
    pub(crate) fn new(
        client: &'c mut Client,
        database: impl Into<String> + Send,
    ) -> QueryBuilder<'c, NoQuery> {
        QueryBuilder {
            client,
            database: database.into(),
            query_text: None,
            query_type: None,
            params: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Builds an SQL query from the given string.
    pub fn sql(self, query_text: impl Into<String>) -> QueryBuilder<'c, Query> {
        // can't use record update syntax because the output type changes
        QueryBuilder {
            client: self.client,
            database: self.database,
            query_text: Some(query_text.into()),
            query_type: Some(QueryType::Sql),
            params: self.params,
            _phantom: PhantomData,
        }
    }

    /// Builds an InfluxQL query from the given string.
    pub fn influxql(self, query_text: impl Into<String>) -> QueryBuilder<'c, Query> {
        // can't use record update syntax because the output type changes
        QueryBuilder {
            client: self.client,
            database: self.database,
            query_text: Some(query_text.into()),
            query_type: Some(QueryType::InfluxQl),
            params: self.params,
            _phantom: PhantomData,
        }
    }
}

impl<'c> QueryBuilder<'c, Query> {
    /// Query the given database with the SQL query constructed by this builder,
    /// returning a struct that can stream Arrow [`arrow::record_batch::RecordBatch`] results.
    pub async fn run(self) -> Result<IOxRecordBatchStream, super::Error> {
        let request = ReadInfo {
            database: self.database,
            sql_query: self.query_text.unwrap(),
            query_type: self.query_type.unwrap().into(),
            flightsql_command: vec![],
            params: self
                .params
                .into_iter()
                .map(|(name, v)| QueryParam {
                    name,
                    value: Some(v.into()),
                })
                .collect(),
            is_debug: false,
        };
        self.client.do_get_with_read_info(request).await
    }
}
