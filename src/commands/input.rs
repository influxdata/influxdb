/// Module to handle input files (and maybe urls?)
use libflate::gzip;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use crate::commands::error::{Error, Result};
use delorean_parquet::ParquetError;

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
        let file = File::open(input_name).map_err(|e| Error::UnableToReadInput {
            name: String::from(input_name),
            source: e,
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| Error::UnableToReadInput {
                name: String::from(input_name),
                source: e,
            })?
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
        let ext = path
            .extension()
            .ok_or(Error::UnknownInputType {
                details: String::from("No extension"),
                input_name: path.display().to_string(),
            })?
            .to_str()
            .ok_or(Error::FileNameDecode {
                input_name: path.display().to_string(),
            })?;

        match ext {
            "tsm" => Ok(Self::FileInputType(FileInputReader::new(
                FileType::TSM,
                input_name,
            )?)),
            "lp" => Ok(Self::FileInputType(FileInputReader::new(
                FileType::LineProtocol,
                input_name,
            )?)),
            "parquet" => Ok(Self::FileInputType(FileInputReader::new(
                FileType::Parquet,
                input_name,
            )?)),
            "gz" => {
                let stem = Path::new(path.file_stem().unwrap());

                let stem_ext = stem
                    .extension()
                    .ok_or(Error::UnknownInputType {
                        details: String::from("No extension before .gz"),
                        input_name: path.display().to_string(),
                    })?
                    .to_str()
                    .ok_or(Error::FileNameDecode {
                        input_name: path.display().to_string(),
                    })?;

                let file = File::open(input_name).map_err(|e| Error::UnableToReadInput {
                    name: input_name.to_string(),
                    source: e,
                })?;
                let mut decoder =
                    gzip::Decoder::new(file).map_err(|gzip_err| Error::UnableToReadInput {
                        name: input_name.to_string(),
                        source: gzip_err,
                    })?;
                let mut buffer = Vec::new();
                decoder
                    .read_to_end(&mut buffer)
                    .map_err(|e| Error::ReadingGzip {
                        input_name: input_name.to_string(),
                        source: e,
                    })?;

                match stem_ext {
                    "tsm" => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::TSM,
                        buffer,
                    ))),
                    "lp" => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::LineProtocol,
                        buffer,
                    ))),
                    "parquet" => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::Parquet,
                        buffer,
                    ))),
                    _ => Err(Error::UnknownInputType {
                        details: String::from("Unknown input extension before .gz"),
                        input_name: input_name.to_string(),
                    }),
                }
            }
            _ => Err(Error::UnknownInputType {
                details: String::from("Unknown input extension"),
                input_name: input_name.to_string(),
            }),
        }
    }
}
