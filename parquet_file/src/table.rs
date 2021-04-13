use snafu::{ResultExt, Snafu};
use std::mem;

use data_types::partition_metadata::TableSummary;
use internal_types::{schema::{builder::SchemaBuilder, Schema}, selection::Selection};
use object_store::path::Path;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing table '{}'", table_name))]
    TableWrite {
        table_name: String,
    },

    #[snafu(display("Table Error in '{}'", table_name))]
    NamedTableError {
        table_name: String,
    },

    #[snafu(display("Table '{}' not found in chunk {}", table_name, chunk_id))]
    NamedTableNotFoundInChunk { table_name: String, chunk_id: u64 },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema {
        source: internal_types::schema::builder::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


/// Table that belongs to a chunk persisted in a parquet file in object store
#[derive(Debug, Clone)]
pub struct Table {
    /// Meta data of the table
    table_summary: TableSummary,

    /// Path in the object store. Format:
    ///  <writer id>/<database>/data/<partition key>/<chunk
    /// id>/<tablename>.parquet
    object_store_path: Path,
}

impl Table {
    pub fn new(meta: TableSummary, path: Path) -> Self {
        Self {
            table_summary: meta,
            object_store_path: path,
        }
    }

    pub fn table_summary(&self) -> TableSummary {
        self.table_summary.clone()
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        self.table_summary.has_table(table_name)
    }

    /// Return the approximate memory size of the table
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.table_summary.size()
            + mem::size_of_val(&self.object_store_path)
    }

    /// Return name of this table
    pub fn name(&self) -> String {
        self.table_summary.name.clone()
    }

    /// Return the object store path of this table
    pub fn path(&self) -> Path {
        self.object_store_path.clone()
    }


    /// Return all columns of this table
    // pub fn all_columns_selection(&self) -> Result<TableColSelection<'a>> {
    //     // TODO
    //     let cols: Vec<ColSelection> = vec![];
    //     let selection = TableColSelection { cols };

    //     // sort so the columns always come out in a predictable name
    //     Ok(selection.sort_by_name())
    // }

    // /// Returns a column selection for just the specified columns
    // fn specific_columns_selection<'a>(
    //     &self,
    //     columns: &'a [&'a str],
    // ) -> Result<TableColSelection<'a>> {
    //     // TODO
    //     let cols: Vec<ColSelection> = vec![];

    //     Ok(TableColSelection { cols })
    // }

    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {

        let mut schema_builder = SchemaBuilder::new();

        // // TODO: maybe just refactor MB's corresponding one
        // for col in &selection.cols {
        //     let column_name = col.column_name;
        //     let column = self.column(col.column_id)?;

        //     schema_builder = match column {
        //         Column::String(_, _) => schema_builder.field(column_name, ArrowDataType::Utf8),
        //         Column::Tag(_, _) => schema_builder.tag(column_name),
        //         Column::F64(_, _) => schema_builder.field(column_name, ArrowDataType::Float64),
        //         Column::I64(_, _) => {
        //             if column_name == TIME_COLUMN_NAME {
        //                 schema_builder.timestamp()
        //             } else {
        //                 schema_builder.field(column_name, ArrowDataType::Int64)
        //             }
        //         }
        //         Column::U64(_, _) => schema_builder.field(column_name, ArrowDataType::UInt64),
        //         Column::Bool(_, _) => schema_builder.field(column_name, ArrowDataType::Boolean),
        //     };
        // }

        schema_builder.build().context(InternalSchema)

        
        // translate chunk selection into name/indexes:
        // let selection = match selection {
        //     Selection::All => self.all_columns_selection(),
        //     Selection::Some(cols) => self.specific_columns_selection(cols),
        // }?;
        // self.schema_impl(&selection)
    }

    // fn schema_impl(&self, selection: &TableColSelection<'_>) -> Result<Schema> {
    //     let mut schema_builder = SchemaBuilder::new();

    //     // TODO: maybe just refactor MB's corresponding one

    //     schema_builder.build().context(InternalSchema)
    // }
}
