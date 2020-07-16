use delorean_parquet::ParquetError;
use delorean_table::Name;
/// Module to handle input files (and maybe urls?)
use libflate::gzip;
use snafu::{ResultExt, Snafu};
use std::{
    borrow::Cow,
    collections::VecDeque,
    fs,
    fs::File,
    io,
    io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
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

    #[snafu(display("Error creating decompressor for {} ({})", input_name.display(), source))]
    UnableToCreateDecompressor {
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
    TSM,
    Parquet,
}

/// Represents an input path and can produce InputReaders for each
/// file seen while recursively traversing the path
pub struct InputPath {
    // All files left to traverse. Elements of files were actual files
    // when this InputPath was constructed.
    files: VecDeque<PathBuf>,
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
    cursor: Cursor<Vec<u8>>,
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

                let path = PathBuf::from(input_name);
                let stem = Path::new(path.file_stem().unwrap());
                let stem_ext = stem.extension().and_then(|p| p.to_str());

                match stem_ext {
                    Some("tsm") => Ok(Self::MemoryInputType(MemoryInputReader::new(
                        FileType::TSM,
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
    pub fn new<P>(root_path: &str, mut pred: P) -> Result<Self>
    where
        P: FnMut(&Path) -> bool,
    {
        struct PathAndType {
            path: PathBuf,
            file_type: fs::FileType,
        };

        let mut paths = VecDeque::new();
        let root_path = PathBuf::from(root_path);
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
            } else if file_type.is_file() && pred(&path) {
                files.push(path)
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
        Ok(Self {
            files: files.into(),
        })
    }
}

/// Owning itertor over items in InputReader
pub struct IntoIter {
    inner: VecDeque<PathBuf>,
}

impl Iterator for IntoIter {
    type Item = Result<InputReader>;

    fn next(&mut self) -> Option<Result<InputReader>> {
        self.inner
            .pop_back()
            .map(|p| InputReader::new(&p.to_string_lossy()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.inner.len();
        (len, Some(len))
    }
}

impl IntoIterator for InputPath {
    type Item = Result<InputReader>;
    type IntoIter = IntoIter;

    /// Consumes the `VecDeque` into a front-to-back iterator yielding elements by
    /// value.
    fn into_iter(self) -> IntoIter {
        IntoIter { inner: self.files }
    }
}
