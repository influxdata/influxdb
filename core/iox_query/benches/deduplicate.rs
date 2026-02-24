// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, StringDictionaryBuilder, TimestampNanosecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::sort_batch;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use iox_query::provider::DeduplicateExec;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn dedupe(c: &mut Criterion) {
    // Single tag with low and high duplicate rates
    let mut generator = DataGenerator::new()
        .with_null_percent(0.00)
        .with_num_tag_values(100_000_000)
        .with_num_rows(100_000);
    let scenario = generator
        .one_tag_two_fields()
        .with_expected_output_count(94945);
    c.bench_function("single tag, 100k input rows, 5% dupe, no nulls", |b| {
        b.iter(|| scenario.run())
    });

    let mut generator = DataGenerator::new()
        .with_null_percent(0.05)
        .with_num_tag_values(100_000)
        .with_num_rows(100_000);
    let scenario = generator
        .one_tag_two_fields()
        .with_expected_output_count(61220);
    c.bench_function("single tag, 100k rows, 50% dupe, 5% null", |b| {
        b.iter(|| scenario.run())
    });

    // multi tag with low/high duplicate rates
    let mut generator = DataGenerator::new()
        .with_null_percent(0.001)
        .with_num_tag_values(100_000)
        .with_num_rows(100_000);
    let scenario = generator
        .five_tag_two_fields()
        .with_expected_output_count(100000);
    c.bench_function("five tags, 100k rows, 0% dupe, 0.1% null", |b| {
        b.iter(|| scenario.run())
    });

    let mut generator = DataGenerator::new()
        .with_null_percent(0.001)
        .with_num_tag_values(8) // 5 distinct values per tag
        .with_num_rows(100_000);
    let scenario = generator
        .five_tag_two_fields()
        .with_expected_output_count(44453);
    c.bench_function("five tags, 100k rows, 55%% dupe, 0.1% null", |b| {
        b.iter(|| scenario.run())
    });
}

criterion_group!(benches, dedupe);
criterion_main!(benches);

// Support Code

struct DataGenerator {
    /// The underlying random number generator
    rng: StdRng,
    /// The percentage of nulls to generate. Defaults to 0.5
    null_percent: f64,
    /// the number of distinct tag values to generate in each column. Defaults to 10
    num_tag_values: usize,
    /// The total number of rows to generate
    num_rows: usize,
}

impl DataGenerator {
    fn new() -> Self {
        Self {
            // Fixed seed for reproducibility
            rng: StdRng::seed_from_u64(42),
            null_percent: 0.05,
            num_tag_values: 10,
            num_rows: 100_000,
        }
    }

    /// set the null percentage
    fn with_null_percent(mut self, null_percent: f64) -> Self {
        self.null_percent = null_percent;
        self
    }

    /// Set the number of distinct tag values
    fn with_num_tag_values(mut self, num_tag_values: usize) -> Self {
        self.num_tag_values = num_tag_values;
        self
    }

    /// Set the number of rows to generate
    fn with_num_rows(mut self, num_rows: usize) -> Self {
        self.num_rows = num_rows;
        self
    }

    /// return a tag value with given null probability and number of distinct values
    fn tag_value(&mut self) -> Option<usize> {
        // 5% nulls
        if self.rng.random_range(0.0..1.0) > 0.05 {
            let tag_num = self.rng.random_range(0..self.num_tag_values);
            Some(tag_num)
        } else {
            None
        }
    }

    /// return a tag column
    fn tag_column(&mut self) -> DictionaryArray<Int32Type> {
        let (tag, _time) = self.tag_time_columns();
        tag
    }

    /// Returns a tag column and a time column using the same algorithm as `tag_column` (so tag and time have the same values)
    fn tag_time_columns(&mut self) -> (DictionaryArray<Int32Type>, TimestampNanosecondArray) {
        let mut temp = String::new();

        let mut builder = StringDictionaryBuilder::new();
        let mut time_builder = TimestampNanosecondArray::builder(self.num_rows);
        for _ in 0..self.num_rows {
            if let Some(tag_num) = self.tag_value() {
                use std::fmt::Write;
                write!(&mut temp, "value{tag_num}").unwrap();
                builder.append_value(&temp);
                temp.clear();
                time_builder.append_value(tag_num as i64);
            } else {
                builder.append_null();
                time_builder.append_null();
            }
        }
        (builder.finish(), time_builder.finish())
    }

    /// Return a random field value
    fn field_value(&mut self) -> f64 {
        self.rng.random::<f64>()
    }

    /// Return a field column
    fn field_column(&mut self) -> Float64Array {
        Float64Array::from_iter((0..self.num_rows).map(|_| self.field_value()))
    }

    /// Returns a set of batches like this:
    /// tag1, field1, field2
    fn one_tag_two_fields(&mut self) -> TestScenario {
        let (tag1, time) = self.tag_time_columns();

        let batch = RecordBatch::try_from_iter(vec![
            ("tag1", Arc::new(tag1) as ArrayRef),
            ("field1", Arc::new(self.field_column())),
            ("field2", Arc::new(self.field_column())),
            ("time", Arc::new(time)),
        ])
        .unwrap();
        let schema = batch.schema();
        let sort_keys = vec![sort_key("tag1", &schema), sort_key("time", &schema)];
        TestScenario::new(batch, sort_keys)
    }

    /// Returns a set of batches like this:
    /// tag1, tag2, tag3, tag4, tag5, field1, field2
    fn five_tag_two_fields(&mut self) -> TestScenario {
        let (tag1, time) = self.tag_time_columns();
        let batch = RecordBatch::try_from_iter(vec![
            ("tag1", Arc::new(tag1) as ArrayRef),
            ("tag2", Arc::new(self.tag_column()) as ArrayRef),
            ("tag3", Arc::new(self.tag_column()) as ArrayRef),
            ("tag4", Arc::new(self.tag_column()) as ArrayRef),
            ("tag5", Arc::new(self.tag_column()) as ArrayRef),
            ("field1", Arc::new(self.field_column())),
            ("field2", Arc::new(self.field_column())),
            ("time", Arc::new(time)),
        ])
        .unwrap();
        let schema = batch.schema();
        let sort_keys = vec![
            sort_key("tag1", &schema),
            sort_key("tag2", &schema),
            sort_key("tag3", &schema),
            sort_key("tag4", &schema),
            sort_key("tag5", &schema),
            sort_key("time", &schema),
        ];
        TestScenario::new(batch, sort_keys)
    }
}
fn sort_key(col_name: &str, schema: &Schema) -> PhysicalSortExpr {
    let options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    PhysicalSortExpr::new(col(col_name, schema).unwrap(), options)
}

/// Test scenario holds all state needed to run a test
struct TestScenario {
    input: Arc<dyn ExecutionPlan>,
    sort_keys: Vec<PhysicalSortExpr>,
    ctx: SessionContext,
    runtime: Runtime,
    // expected output count, to verify that the scenario is covering what we expect
    expected_output_count: Option<usize>,
}

impl TestScenario {
    /// Create a test, with the given input and sort keys
    fn new(input: RecordBatch, sort_keys: Vec<PhysicalSortExpr>) -> Self {
        let num_rows = input.num_rows();
        // First sort the input data by tags
        let fetch = None;
        let mut batch =
            sort_batch(&input, &LexOrdering::new(sort_keys.clone()).unwrap(), fetch).unwrap();

        // Divide into batches of 4k rows
        const BATCH_SIZE: usize = 4000;
        let mut batches = vec![];
        while batch.num_rows() > BATCH_SIZE {
            let new_batch = batch.slice(0, BATCH_SIZE);
            batches.push(new_batch);
            batch = batch.slice(BATCH_SIZE, batch.num_rows() - BATCH_SIZE);
        }
        batches.push(batch);
        // make sure we didn't lose any rows
        assert_eq!(
            num_rows,
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        // Setup in memory stream
        let schema = batches[0].schema();
        let projection = None;
        let partitions = vec![batches];
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, projection).unwrap(),
        )));

        // run using a single thread
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let ctx = SessionContext::new();
        Self {
            input,
            sort_keys,
            ctx,
            runtime,
            expected_output_count: None,
        }
    }

    /// Set the expected output count
    fn with_expected_output_count(mut self, expected_output_count: usize) -> Self {
        self.expected_output_count = Some(expected_output_count);
        self
    }

    /// Run `DeduplicateExec` on the input data
    fn run(&self) {
        let use_chunk_order_col = false;
        let exec = Arc::new(DeduplicateExec::new(
            Arc::clone(&self.input),
            LexOrdering::new(self.sort_keys.clone()).unwrap(),
            use_chunk_order_col,
        )) as _;
        self.runtime.block_on(async move {
            let output = collect(exec, self.ctx.task_ctx()).await.unwrap();
            assert!(!output.is_empty());
            // verify the expected output count
            let output_count = output.iter().map(|b| b.num_rows()).sum::<usize>();
            let expected_output_count = self
                .expected_output_count
                .expect("expected_output_count not set");
            assert_eq!(output_count, expected_output_count);
        });
    }
}
