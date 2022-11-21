mod reader;
pub use reader::{ClosedSegmentFileReader, Error as ReaderError, Result as ReaderResult};

mod writer;
pub use writer::{Error as WriterError, OpenSegmentFileWriter, Result as WriterResult};
