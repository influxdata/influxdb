use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{plan_err, Result},
    datasource::{function::TableFunctionImpl, TableProvider, TableType},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    scalar::ScalarValue,
};

use super::LastCacheProvider;

struct LastCacheFunctionProvider {
    db_name: String,
    table_name: String,
    cache_name: String,
    schema: SchemaRef,
    provider: Arc<LastCacheProvider>,
}

#[async_trait]
impl TableProvider for LastCacheFunctionProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let read = self.provider.cache_map.read();
        let batches = if let Some(cache) = read
            .get(&self.db_name)
            .and_then(|db| db.get(&self.table_name))
            .and_then(|tbl| tbl.get(&self.cache_name))
        {
            let predicates = cache.convert_filter_exprs(filters);
            cache.to_record_batches(&predicates)?
        } else {
            // If there is no cache, it means that it was removed, in which case, we just return
            // an empty set of record batches.
            vec![]
        };
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
        let Some(Expr::Literal(ScalarValue::Utf8(Some(table_name)))) = args.first() else {
            return plan_err!("first argument must be the table name as a string");
        };

        let cache_name = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)))) => Some(name),
            Some(_) => {
                return plan_err!("second argument, if passed, must be the cache name as a string")
            }
            None => None,
        };

        match self.provider.get_cache_name_and_schema(
            &self.db_name,
            table_name,
            cache_name.map(|x| x.as_str()),
        ) {
            Some((cache_name, schema)) => Ok(Arc::new(LastCacheFunctionProvider {
                db_name: self.db_name.clone(),
                table_name: table_name.clone(),
                cache_name,
                schema,
                provider: Arc::clone(&self.provider),
            })),
            None => plan_err!("could not find cache for the given arguments"),
        }
    }
}
