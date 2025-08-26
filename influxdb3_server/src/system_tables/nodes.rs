use std::sync::Arc;

use arrow::array::{
    GenericListBuilder, StringViewBuilder, TimestampMillisecondBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_catalog::catalog::{Catalog, NodeDefinition};
use iox_system_tables::IoxSystemTable;
use tonic::async_trait;

#[derive(Debug)]
pub(crate) struct NodeSystemTable {
    catalog: Arc<Catalog>,
    schema: SchemaRef,
}

impl NodeSystemTable {
    pub(crate) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            schema: table_schema(),
        }
    }
}

#[async_trait]
impl IoxSystemTable for NodeSystemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let nodes = self.catalog.list_nodes();
        to_record_batch(&self.schema, nodes)
    }
}

fn table_schema() -> SchemaRef {
    let fields = vec![
        Field::new("node_id", DataType::Utf8View, false),
        Field::new("node_catalog_id", DataType::UInt32, false),
        Field::new("instance_id", DataType::Utf8View, false),
        Field::new(
            "mode",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
            false,
        ),
        Field::new("core_count", DataType::UInt64, false),
        Field::new("state", DataType::Utf8View, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("cli_params", DataType::Utf8View, true),
    ];
    Arc::new(Schema::new(fields))
}

fn to_record_batch(
    schema: &SchemaRef,
    nodes: Vec<Arc<NodeDefinition>>,
) -> Result<RecordBatch, DataFusionError> {
    let cap = nodes.len();
    let mut node_id_builder = StringViewBuilder::with_capacity(cap);
    let mut node_catalog_id_builder = UInt32Builder::with_capacity(cap);
    let mut instance_id_builder = StringViewBuilder::with_capacity(cap);
    let mode_builder = StringViewBuilder::new();
    let mut mode_builder =
        GenericListBuilder::<i32, StringViewBuilder>::with_capacity(mode_builder, cap);
    let mut core_count_builder = UInt64Builder::with_capacity(cap);
    let mut state_builder = StringViewBuilder::with_capacity(cap);
    let mut updated_at_builder = TimestampMillisecondBuilder::with_capacity(cap);
    let mut cli_params_builder = StringViewBuilder::with_capacity(cap);

    for node in &nodes {
        node_id_builder.append_value(node.node_id());
        node_catalog_id_builder.append_value(node.node_catalog_id().get());
        instance_id_builder.append_value(node.instance_id());
        for mode in node.modes() {
            mode_builder.values().append_value(mode.as_str());
        }
        mode_builder.append(true);
        core_count_builder.append_value(node.core_count());
        state_builder.append_value(node.state().as_str());
        updated_at_builder.append_value(node.state().updated_at_ns() / (1_000 * 1_000));

        // Add cli_params - handle None case
        if let Some(params) = node.cli_params() {
            cli_params_builder.append_value(params);
        } else {
            cli_params_builder.append_null();
        }
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(node_id_builder.finish()),
            Arc::new(node_catalog_id_builder.finish()),
            Arc::new(instance_id_builder.finish()),
            Arc::new(mode_builder.finish()),
            Arc::new(core_count_builder.finish()),
            Arc::new(state_builder.finish()),
            Arc::new(updated_at_builder.finish()),
            Arc::new(cli_params_builder.finish()),
        ],
    )
    .map_err(Into::into)
}
