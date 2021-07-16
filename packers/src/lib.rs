#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod packers;
pub mod sorter;
pub mod stats;

use snafu::Snafu;

pub use crate::packers::{Packer, Packers};
use internal_types::schema::Schema;
pub use parquet::data_type::ByteArray;

use std::borrow::Cow;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Data Error: {}"#, source))]
    Data {
        source: Box<dyn std::error::Error>,
    },

    #[snafu(display(r#"IO Error: {} ({})"#, message, source,))]
    Io {
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
        Self::Io {
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
pub trait IOxTableWriter {
    /// Writes a batch of packed data to the underlying output
    fn write_batch(&mut self, packers: &[Packers]) -> Result<(), Error>;

    /// Closes the underlying writer and finalizes the work to write the file.
    fn close(&mut self) -> Result<(), Error>;
}

/// Something that can  instantiate a `IOxTableWriter`
pub trait IOxTableWriterSource {
    /// Returns a `IOxTableWriter suitable for writing data from packers.
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn IOxTableWriter>, Error>;
}

/// Ergonomics: implement IOxTableWriter for Box'd values
impl<S> IOxTableWriterSource for Box<S>
where
    S: IOxTableWriterSource + ?Sized,
{
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn IOxTableWriter>, Error> {
        (**self).next_writer(schema)
    }
}

pub trait Name {
    /// Returns a user understandable identifier of this thing
    fn name(&self) -> Cow<'_, str>;
}
