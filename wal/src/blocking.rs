mod reader;
pub use reader::{Error as ReaderError, Result as ReaderResult, SegmentFileReader};

mod writer;
pub use writer::{Error as WriterError, Result as WriterResult, SegmentFileWriter};
