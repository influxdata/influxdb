use delorean_parquet::ParquetError;
/// Module to handle input files (and maybe urls?)
use libflate::gzip;
use snafu::{ResultExt, Snafu};
use std::{
    fs::File,
    io,
    io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum FileType {
    LineProtocol,
    TSM,
    Parquet,
}

// Interface for interacting with streams
#[derive(Debug)]
pub enum InputReader {
    FileInputType(FileInputReader),
    MemoryInputType(MemoryInputReader),
}

// Contains a (file backed) reader to read raw uncompressed bytes
#[derive(Debug)]
pub struct FileInputReader {
    file_type: FileType,
    file_size: u64,
    reader: BufReader<std::fs::File>,
}

// Contains an in-memory reader...
#[derive(Debug)]
pub struct MemoryInputReader {
    file_type: FileType,
    file_size: u64,
    cursor: Cursor<Vec<u8>>,
}

impl FileInputReader {
    fn new(file_type: FileType, input_name: &str) -> Result<Self> {
        let file = File::open(input_name).context(UnableToOpenInput { input_name })?;

        let file_size = file
            .metadata()
            .context(UnableToCalculateSize { input_name })?
            .len();

        Ok(Self {
            file_type,
            file_size,
            reader: BufReader::new(file),
        })
    }
}

impl MemoryInputReader {
    fn new(file_type: FileType, buffer: Vec<u8>) -> Self {
        let len = buffer.len();
        Self {
            file_type,
            file_size: len as u64,
            cursor: Cursor::new(buffer),
        }
    }
}

impl Seek for InputReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.reader.seek(pos),
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.cursor.seek(pos),
        }
    }
}

impl Read for InputReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.reader.read(buf),
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.cursor.read(buf),
        }
    }
}

impl delorean_parquet::Length for InputReader {
    fn len(&self) -> u64 {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.file_size,
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.file_size,
        }
    }
}

impl delorean_parquet::TryClone for InputReader {
    fn try_clone(&self) -> std::result::Result<Self, ParquetError> {
        Err(ParquetError::NYI(String::from("TryClone for input reader")))
    }
}

impl BufRead for InputReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.reader.fill_buf(),
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.cursor.fill_buf(),
        }
    }
    fn consume(&mut self, amt: usize) {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.reader.consume(amt),
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.cursor.consume(amt),
        }
    }
}

impl InputReader {
    pub fn file_type(&self) -> &FileType {
        match self {
            Self::FileInputType(file_input_reader) => &file_input_reader.file_type,
            Self::MemoryInputType(memory_input_reader) => &memory_input_reader.file_type,
        }
    }

    pub fn len(&self) -> u64 {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.file_size,
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.file_size,
        }
    }

    // Create a new input reader suitable for reading from
    // `input_name` and figures out the file input type based on
    // heuristics (ahem, the filename extension)
    pub fn new(input_name: &str) -> Result<Self> {
        let path = Path::new(input_name);

        // Initially simply use the file name's extension to determine
        // the type; Maybe in the future we can be more clever and
        // inspect contents.
        let ext = path.extension().and_then(|p| p.to_str());

        match ext {
            Some("tsm") => Ok(Self::FileInputType(FileInputReader::new(
                FileType::TSM,
                input_name,
            )?)),
            Some("lp") => Ok(Self::FileInputType(FileInputReader::new(
                FileType::LineProtocol,
                input_name,
            )?)),
            Some("parquet") => Ok(Self::FileInputType(FileInputReader::new(
                FileType::Parquet,
                input_name,
            )?)),
            Some("gz") => {
                let buffer = || {
                    let file = File::open(input_name).context(UnableToOpenInput { input_name })?;
                    let mut decoder = gzip::Decoder::new(file)
                        .context(UnableToCreateDecompressor { input_name })?;
                    let mut buffer = Vec::new();
                    decoder
                        .read_to_end(&mut buffer)
                        .context(ReadingGzip { input_name })?;
                    Ok(buffer)
                };

                let stem = Path::new(path.file_stem().unwrap());
                let stem_ext = stem.extension().and_then(|p| p.to_str());

                match stem_ext {
                    Some("tsm") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::TSM,
                        buffer()?,
                    ))),
                    Some("lp") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::LineProtocol,
                        buffer()?,
                    ))),
                    Some("parquet") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::Parquet,
                        buffer()?,
                    ))),
                    _ => UnknownInputTypeGzip { input_name }.fail(),
                }
            }
            _ => UnknownInputType { input_name }.fail(),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening {} ({})", input_name.display(), source))]
    UnableToOpenInput {
        input_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error calculating the size of {} ({})", input_name.display(), source))]
    UnableToCalculateSize {
        input_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error creating decompressor for {} ({})", input_name.display(), source))]
    UnableToCreateDecompressor {
        input_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unknown input type: {} has an unknown input extension before .gz", input_name.display()))]
    UnknownInputTypeGzip { input_name: PathBuf },

    #[snafu(display("Unknown input type: {} has an unknown input extension", input_name.display()))]
    UnknownInputType { input_name: PathBuf },

    #[snafu(display("Can't read GZip data: {}", input_name.display()))]
    ReadingGzip {
        input_name: PathBuf,
        source: std::io::Error,
    },
}
