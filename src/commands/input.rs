use std::{
    borrow::Cow,
    collections::VecDeque,
    fs,
    fs::File,
    io,
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use snafu::{ResultExt, Snafu};

/// Module to handle input files (and maybe urls?)
use packers::Name;
use parquet::{
    self,
    file::{
        reader::ChunkReader,
        serialized_reader::{FileSource, SliceableCursor},
        writer::TryClone,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening {} ({})", input_name.display(), source))]
    UnableToOpenInput {
        input_name: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Error reading directory {} ({})", input_name.display(), source))]
    UnableToReadDirectory {
        input_name: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Error calculating the size of {} ({})", input_name.display(), source))]
    UnableToCalculateSize {
        input_name: PathBuf,
        source: io::Error,
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

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    LineProtocol,
    Tsm,
    Parquet,
}

/// Represents an input path and can produce InputReaders for each
/// file seen while recursively traversing the path
pub struct InputPath {
    // All files left to traverse. Elements of files were actual files
    // when this InputPath was constructed.
    files: Vec<PathBuf>,
}

/// Interface for interacting with streams
#[derive(Debug)]
pub enum InputReader {
    FileInputType(FileInputReader),
    MemoryInputType(MemoryInputReader),
}

/// A (file backed) reader to read raw uncompressed bytes
#[derive(Debug)]
pub struct FileInputReader {
    file_type: FileType,
    file_size: u64,
    path: PathBuf,
    reader: BufReader<std::fs::File>,
}

/// An in-memory reader
#[derive(Debug)]
pub struct MemoryInputReader {
    file_type: FileType,
    file_size: u64,
    path: PathBuf,
    cursor: SliceableCursor,
}

impl FileInputReader {
    fn new(file_type: FileType, input_name: &str) -> Result<Self> {
        let path = PathBuf::from(input_name);
        let file = File::open(&path).context(UnableToOpenInput { input_name })?;

        let file_size = file
            .metadata()
            .context(UnableToCalculateSize { input_name })?
            .len();

        Ok(Self {
            file_type,
            file_size,
            path,
            reader: BufReader::new(file),
        })
    }
}

impl MemoryInputReader {
    fn new(file_type: FileType, path: PathBuf, buffer: Vec<u8>) -> Self {
        let len = buffer.len();
        Self {
            file_type,
            file_size: len as u64,
            path,
            cursor: SliceableCursor::new(buffer),
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

impl parquet::file::reader::Length for InputReader {
    fn len(&self) -> u64 {
        match self {
            Self::FileInputType(file_input_reader) => file_input_reader.file_size,
            Self::MemoryInputType(memory_input_reader) => memory_input_reader.file_size,
        }
    }
}

impl ChunkReader for InputReader {
    type T = InputSlice;
    fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<Self::T> {
        match self {
            Self::FileInputType(file_input_reader) => Ok(InputSlice::FileSlice(FileSource::new(
                file_input_reader.reader.get_ref(),
                start,
                length,
            ))),
            Self::MemoryInputType(memory_input_reader) => Ok(InputSlice::Memory(
                memory_input_reader.cursor.get_read(start, length)?,
            )),
        }
    }
}

impl TryClone for InputReader {
    fn try_clone(&self) -> std::result::Result<Self, std::io::Error> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "TryClone for input reader not supported",
        ))
    }
}

impl Name for InputReader {
    /// Returns a user understandable identifier of this thing
    fn name(&self) -> Cow<'_, str> {
        self.path().to_string_lossy()
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

    pub fn path(&self) -> &Path {
        match self {
            Self::FileInputType(file_input_reader) => &file_input_reader.path,
            Self::MemoryInputType(memory_input_reader) => &memory_input_reader.path,
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
                FileType::Tsm,
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
                    let mut decoder = flate2::read::GzDecoder::new(file);
                    let mut buffer = Vec::new();
                    decoder
                        .read_to_end(&mut buffer)
                        .context(ReadingGzip { input_name })?;
                    Ok(buffer)
                };

                let path = PathBuf::from(input_name);
                let stem = Path::new(path.file_stem().unwrap());
                let stem_ext = stem.extension().and_then(|p| p.to_str());

                match stem_ext {
                    Some("tsm") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::Tsm,
                        path,
                        buffer()?,
                    ))),
                    Some("lp") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::LineProtocol,
                        path,
                        buffer()?,
                    ))),
                    Some("parquet") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::Parquet,
                        path,
                        buffer()?,
                    ))),
                    _ => UnknownInputTypeGzip { input_name }.fail(),
                }
            }
            _ => UnknownInputType { input_name }.fail(),
        }
    }
}

impl InputPath {
    // Create a new InputPath with a snapshot of all the files in the
    // directory tree rooted at root_path that pass predicate P
    pub fn new<P>(root_path: impl Into<PathBuf>, mut pred: P) -> Result<Self>
    where
        P: FnMut(&Path) -> bool,
    {
        struct PathAndType {
            path: PathBuf,
            file_type: fs::FileType,
        }

        let mut paths = VecDeque::new();
        let root_path = root_path.into();
        let root_meta = fs::metadata(&root_path).context(UnableToOpenInput {
            input_name: root_path.clone(),
        })?;

        paths.push_back(PathAndType {
            path: root_path,
            file_type: root_meta.file_type(),
        });

        let mut files = Vec::with_capacity(100);
        while let Some(PathAndType { path, file_type }) = paths.pop_front() {
            if file_type.is_dir() {
                let dir = fs::read_dir(&path).context(UnableToReadDirectory {
                    input_name: path.clone(),
                })?;

                for entry in dir {
                    let entry = entry.context(UnableToReadDirectory {
                        input_name: path.clone(),
                    })?;

                    let file_type = entry.file_type().context(UnableToOpenInput {
                        input_name: entry.path(),
                    })?;

                    paths.push_back(PathAndType {
                        path: entry.path(),
                        file_type,
                    });
                }
            } else if file_type.is_file() {
                if pred(&path) {
                    files.push(path)
                }
            } else {
                unimplemented!(
                    "Unknown file type {:?} while recursing {:?}",
                    file_type,
                    path
                );
            }
        }

        // sort filenames in order to ensure repeatability between runs
        files.sort();
        Ok(Self { files })
    }

    pub fn input_readers(&self) -> impl Iterator<Item = Result<InputReader>> + '_ {
        self.files
            .iter()
            .rev()
            .map(|p| InputReader::new(&p.to_string_lossy()))
    }
}

pub enum InputSlice {
    FileSlice(FileSource<File>),
    Memory(SliceableCursor),
}

impl Read for InputSlice {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::FileSlice(src) => src.read(buf),
            Self::Memory(src) => src.read(buf),
        }
    }
}
