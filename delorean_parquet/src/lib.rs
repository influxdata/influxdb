//! This module contains code for writing / reading delorean data to parquet.
#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use parquet::{
    errors::ParquetError,
    file::reader::{Length, TryClone},
};
use std::io::{Read, Seek, SeekFrom};

pub mod error;
pub mod metadata;
pub mod writer;

/// Thing that adapts an object that implements Read+Seek to something
/// that also implements the parquet TryClone interface, required by
/// the parquet reader
struct InputReaderAdapter<R>
where
    R: Read + Seek,
{
    real_reader: R,
    size: u64,
}

impl<R: Read + Seek> InputReaderAdapter<R> {
    fn new(real_reader: R, size: u64) -> InputReaderAdapter<R> {
        InputReaderAdapter { real_reader, size }
    }
}

impl<R: Read + Seek> Read for InputReaderAdapter<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.real_reader.read(buf)
    }
}

impl<R: Read + Seek> Seek for InputReaderAdapter<R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        self.real_reader.seek(pos)
    }
}

impl<R: Read + Seek> TryClone for InputReaderAdapter<R> {
    fn try_clone(&self) -> std::result::Result<Self, ParquetError> {
        Err(ParquetError::NYI(String::from("TryClone for input reader")))
    }
}

impl<R: Read + Seek> Length for InputReaderAdapter<R> {
    fn len(&self) -> u64 {
        self.size
    }
}
