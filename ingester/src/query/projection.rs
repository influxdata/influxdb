use arrow::record_batch::RecordBatch;
use mutable_batch::MutableBatch;
use schema::SchemaBuilder;

/// The private inner type to prevent callers from constructing an empty Subset.
#[derive(Debug, Default)]
enum Projection {
    /// Return all columns.
    #[default]
    All,

    /// Return the specified subset of columns.
    ///
    /// The returned columns MAY NOT match the specified column order.
    //
    // Invariant: subset is never empty - this variant is only constructed when
    // there is at least one column to project.
    Project(Vec<String>),
}

/// Specify the set of columns to project during a query.
///
/// Defaults to "all columns".
#[derive(Debug, Default)]
pub(crate) struct OwnedProjection(Projection);

impl From<Vec<String>> for OwnedProjection {
    fn from(value: Vec<String>) -> Self {
        if value.is_empty() {
            return Self(Projection::All);
        }

        Self(Projection::Project(value))
    }
}

impl From<Vec<&str>> for OwnedProjection {
    fn from(value: Vec<&str>) -> Self {
        if value.is_empty() {
            return Self(Projection::All);
        }

        Self(Projection::Project(
            value.into_iter().map(ToString::to_string).collect(),
        ))
    }
}

impl OwnedProjection {
    /// Copy the data within a [`MutableBatch`] into a [`RecordBatch`], applying
    /// the the specified projection.
    ///
    /// This avoids copying column data for columns that are not part of the
    /// projection.
    ///
    /// NOTE: this copies the underlying column data
    pub(crate) fn project_mutable_batches(&self, batch: &MutableBatch) -> RecordBatch {
        // Pre-allocate the outputs to their maximal possible size to avoid
        // reallocations.
        let max_capacity = match &self.0 {
            Projection::All => batch.columns().len(),
            Projection::Project(s) => s.len(),
        };

        let mut schema_builder = SchemaBuilder::with_capacity(max_capacity);
        let mut column_data = Vec::with_capacity(max_capacity);

        // Compute the schema overlap between the requested projection, and the
        // buffered data.
        //
        // Generate the RecordBatch contents in a single pass.
        match &self.0 {
            Projection::All => {
                // If there's no projection, the columns must be emitted ordered
                // by their name.
                let mut columns = batch.columns().collect::<Vec<_>>();
                columns.sort_unstable_by_key(|v| v.0);

                for (name, column) in columns.into_iter() {
                    schema_builder.influx_column(name, column.influx_type());
                    column_data.push(column.to_arrow().expect("failed to snapshot buffer data"));
                }
            }

            Projection::Project(cols) => {
                // Invariant: subset is never empty
                assert!(!cols.is_empty());

                // Construct the schema & data arrays in a single pass, ordered
                // by the projection and ignoring any missing columns.
                for name in cols {
                    if let Ok(column) = batch.column(name) {
                        schema_builder.influx_column(name, column.influx_type());
                        column_data
                            .push(column.to_arrow().expect("failed to snapshot buffer data"));
                    }
                }
            }
        };

        let schema = schema_builder
            .build()
            .expect("failed to create batch schema");

        RecordBatch::try_new(schema.into(), column_data)
            .expect("failed to generate snapshot record batch")
    }

    /// Apply the specified projection to `batches`.
    ///
    /// This projection requires relatively cheap ref-counting clones and does
    /// not copy the underlying data.
    pub(crate) fn project_record_batch(&self, batches: &[RecordBatch]) -> Vec<RecordBatch> {
        match &self.0 {
            Projection::All => batches.to_vec(),
            Projection::Project(columns) => {
                // Invariant: subset is never empty
                assert!(!columns.is_empty());

                batches
                    .iter()
                    .map(|batch| {
                        let schema = batch.schema();

                        // Map the column names to column indexes, ignoring
                        // columns specified in the columns that do not exist
                        // in this batch.
                        let projection = columns
                            .iter()
                            .flat_map(|column_name| schema.index_of(column_name).ok())
                            .collect::<Vec<_>>();

                        batch
                            .project(&projection)
                            .expect("batch projection failure")
                    })
                    .collect()
            }
        }
    }

    /// Return the column names in this projection, if specified.
    pub(crate) fn columns(&self) -> Option<&[String]> {
        match &self.0 {
            Projection::All => None,
            Projection::Project(v) => Some(v.as_ref()),
        }
    }
}
