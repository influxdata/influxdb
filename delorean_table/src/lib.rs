pub mod packers;

use snafu::Snafu;

use delorean_table_schema::Schema;
pub use packers::{Packer, Packers};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Data Error: {}"#, source))]
    Data { source: Box<dyn std::error::Error> },

    #[snafu(display(r#"IO Error: {} ({})"#, message, source,))]
    IO {
        message: String,
        source: std::io::Error,
    },

    #[snafu(display(r#"Other Error: {}"#, source))]
    Other { source: Box<dyn std::error::Error> },
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
