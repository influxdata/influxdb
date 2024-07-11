use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{plan_err, Result},
    datasource::{function::TableFunctionImpl, TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    scalar::ScalarValue,
};

use super::{LastCache, LastCacheProvider};

#[async_trait]
impl TableProvider for LastCache {
    fn as_any(&self) -> &dyn Any {
        &self.inner as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.inner.read().schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicates = self.convert_filter_exprs(filters);
        let batches = self.to_record_batches(&predicates)?;
        let mut exec = MemoryExec::try_new(&[batches], self.schema(), projection.cloned())?;

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

pub struct LastCacheFunction {
    db_name: String,
    provider: Arc<LastCacheProvider>,
}

impl LastCacheFunction {
    pub fn new(db_name: impl Into<String>, provider: Arc<LastCacheProvider>) -> Self {
        Self {
            db_name: db_name.into(),
            provider,
        }
    }
}

impl TableFunctionImpl for LastCacheFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(table_name)))) = args.get(0) else {
            return plan_err!("first argument must be the table name as a string");
        };

        let cache_name = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)))) => Some(name),
            Some(_) => {
                return plan_err!("second argument, if passed, must be the cache name as a string")
            }
            None => None,
        };

        let Ok(last_cache) =
            self.provider
                .get_cache(&self.db_name, &table_name, cache_name.map(|x| x.as_str()))
        else {
            return plan_err!("unable to retrieve last cache using provided arguments");
        };

        Ok(last_cache)
    }
}
