#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod packers;
pub mod stats;

use snafu::Snafu;

use delorean_table_schema::Schema;
pub use packers::{Packer, Packers};
pub use parquet::data_type::ByteArray;

use std::borrow::Cow;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Data Error: {}"#, source))]
    Data {
        source: Box<dyn std::error::Error>,
    },

    #[snafu(display(r#"IO Error: {} ({})"#, message, source,))]
    IO {
        message: String,
        source: std::io::Error,
    },

    #[snafu(display(r#"Other Error: {}"#, source))]
    Other {
        source: Box<dyn std::error::Error>,
    },

    #[snafu(display(r#"Column {:?} had mixed datatypes: {}"#, column_name, details))]
    ColumnWithMixedTypes {
        column_name: Option<String>,
        details: String,
    },

    ColumnStatsBuilderError {
        details: String,
    },
}

impl Error {
    pub fn from_io(source: std::io::Error, message: impl Into<String>) -> Self {
        Self::IO {
            source,
            message: message.into(),
        }
    }

    pub fn from_other(source: impl std::error::Error + 'static) -> Self {
        Self::Other {
            source: Box::new(source),
        }
    }
}

/// Something that knows how to write a set of columns somewhere
pub trait DeloreanTableWriter {
    /// Writes a batch of packed data to the underlying output
    fn write_batch(&mut self, packers: &[Packers]) -> Result<(), Error>;

    /// Closes the underlying writer and finalizes the work to write the file.
    fn close(&mut self) -> Result<(), Error>;
}

/// Something that can  instantiate a `DeloreanTableWriter`
pub trait DeloreanTableWriterSource {
    /// Returns a `DeloreanTableWriter suitable for writing data from packers.
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn DeloreanTableWriter>, Error>;
}

/// Ergonomics: implement DeloreanTableWriterSource for Box'd values
impl<S> DeloreanTableWriterSource for Box<S>
where
    S: DeloreanTableWriterSource + ?Sized,
{
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn DeloreanTableWriter>, Error> {
        (**self).next_writer(schema)
    }
}

pub trait Name {
    /// Returns a user understandable identifier of this thing
    fn name(&self) -> Cow<'_, str>;
}
