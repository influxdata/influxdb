use std::collections::BTreeMap;

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams};

/// Trait for ParquetFiles and ParquetFileParams (which are not yet
/// inserted into the catalog and thus are not assigned an ID)
pub trait ParquetFileInfo {
    fn min_time(&self) -> i64;
    fn max_time(&self) -> i64;
    fn file_size_bytes(&self) -> i64;
    fn compaction_level(&self) -> CompactionLevel;
    /// returns a value like `L0.<id>` to identify this ParquetFile
    fn display_id(&self) -> String;
    fn max_l0_created_at(&self) -> i64;
}

impl ParquetFileInfo for ParquetFile {
    fn min_time(&self) -> i64 {
        self.min_time.get()
    }

    fn max_time(&self) -> i64 {
        self.max_time.get()
    }

    fn file_size_bytes(&self) -> i64 {
        self.file_size_bytes
    }

    fn display_id(&self) -> String {
        let level = display_level(&self.compaction_level());
        let id = self.id;
        format!("{level}.{id}")
    }

    fn compaction_level(&self) -> CompactionLevel {
        self.compaction_level
    }

    fn max_l0_created_at(&self) -> i64 {
        self.max_l0_created_at.get()
    }
}

impl ParquetFileInfo for ParquetFileParams {
    fn min_time(&self) -> i64 {
        self.min_time.get()
    }

    fn max_time(&self) -> i64 {
        self.max_time.get()
    }

    fn file_size_bytes(&self) -> i64 {
        self.file_size_bytes
    }

    fn compaction_level(&self) -> CompactionLevel {
        self.compaction_level
    }

    fn display_id(&self) -> String {
        let level = display_level(&self.compaction_level());
        // ID is not assigned, so use '?' in place of id
        format!("{level}.?")
    }

    fn max_l0_created_at(&self) -> i64 {
        self.max_l0_created_at.get()
    }
}

/// Formats the list of files in the manner described on
/// [`ParquetFileFormatter`] into strings suitable for comparison with
/// `insta`.
pub fn format_files<P: ParquetFileInfo>(title: impl Into<String>, files: &[P]) -> Vec<String> {
    readable_list_of_files(Some(title.into()), files)
}

/// Formats two lists of files in the manner described on
/// [`ParquetFileFormatter`] into strings suitable for comparison with
/// `insta`.
pub fn format_files_split<P: ParquetFileInfo>(
    title1: impl Into<String>,
    files1: &[P],
    title2: impl Into<String>,
    files2: &[P],
) -> Vec<String> {
    let strings1 = readable_list_of_files(Some(title1.into()), files1);
    let strings2 = readable_list_of_files(Some(title2.into()), files2);

    strings1.into_iter().chain(strings2.into_iter()).collect()
}

/// default width for printing
const DEFAULT_WIDTH: usize = 90;

/// default width for header
const DEFAULT_HEADING_WIDTH: usize = 25;

/// This function returns a visual representation of the list of
/// parquet files arranged so they are lined up horizontally based on
/// their relative time range.
///
/// See docs on [`ParquetFileFormatter`] for examples.
fn readable_list_of_files<P: ParquetFileInfo>(title: Option<String>, files: &[P]) -> Vec<String> {
    let mut output = vec![];
    if let Some(title) = title {
        output.push(title);
    }

    if files.is_empty() {
        return output;
    }

    let formatter = ParquetFileFormatter::new(files);

    // split up the files into groups by levels (compaction levels)
    let mut files_by_level = BTreeMap::new();
    for file in files {
        let existing_files = files_by_level
            .entry(file.compaction_level())
            .or_insert_with(Vec::new);
        existing_files.push(file);
    }

    for (level, files) in files_by_level {
        output.push(formatter.format_level(&level));
        for file in files {
            output.push(formatter.format_file(file))
        }
    }

    output
}

/// Formats a parquet files as a single line of text, with widths
/// normalized based on their min/max times and lined up horizontally
/// based on their relative time range.
///
/// Each file has this format:
///
/// ```text
/// L<levelno>.<id>[min_time,max_time] max_l0_created_at @file_size_bytes
/// ```
///
/// Example
///
/// ```text
/// L0
/// L0.1[100,200] 1m @1     |----------L0.1----------|
/// L0.2[300,400] 4m @1                                                          |----------L0.2----------|
/// L0.11[150,350] 15m @44                |-----------------------L0.11-----------------------|
/// ```
#[derive(Debug, Default)]
struct ParquetFileFormatter {
    /// should the size of the files be shown (if they are different)
    file_size_seen: FileSizeSeen,
    /// width in characater
    row_heading_chars: usize,
    /// width, in characters, of the entire min/max timerange
    width_chars: usize,
    /// how many ns are given a single character's width
    ns_per_char: f64,
    /// what is the lowest time range in any file
    min_time: i64,
    /// what is the largest time in any file?
    max_time: i64,
}

#[derive(Debug, Default)]
/// helper to track if there are multiple file sizes in a set of parquet files
enum FileSizeSeen {
    #[default]
    None,
    One(i64),
    Many,
}

impl FileSizeSeen {
    fn observe(self, file_size_bytes: i64) -> Self {
        match self {
            Self::None => Self::One(file_size_bytes),
            // same file size?
            Self::One(sz) if sz == file_size_bytes => Self::One(sz),
            // different file size or already seen difference
            Self::One(_) | Self::Many => Self::Many,
        }
    }
}

impl ParquetFileFormatter {
    /// calculates display parameters for formatting a set of files
    fn new<P: ParquetFileInfo>(files: &[P]) -> Self {
        let row_heading_chars = DEFAULT_HEADING_WIDTH;
        let width_chars = DEFAULT_WIDTH;

        let min_time = files
            .iter()
            .map(|f| f.min_time())
            .min()
            .expect("at least one file");
        let max_time = files
            .iter()
            .map(|f| f.max_time())
            .max()
            .expect("at least one file");
        let file_size_seen = files
            .iter()
            .fold(FileSizeSeen::None, |file_size_seen, file| {
                file_size_seen.observe(file.file_size_bytes())
            });

        let time_range = max_time - min_time;

        let ns_per_char = (time_range as f64) / (width_chars as f64);

        Self {
            file_size_seen,
            width_chars,
            ns_per_char,
            min_time,
            max_time,
            row_heading_chars,
        }
    }

    /// return how many characters of `self.width_chars` would be consumed by `range` ns
    fn time_range_to_chars(&self, time_range: i64) -> usize {
        // avoid divide by zero
        if self.ns_per_char > 0.0 {
            (time_range as f64 / self.ns_per_char) as usize
        } else if time_range > 0 {
            self.width_chars
        } else {
            0
        }
    }

    fn format_level(&self, level: &CompactionLevel) -> String {
        let level_heading = display_level(level);
        let level_heading = match self.file_size_seen {
            FileSizeSeen::One(sz) => {
                let sz = display_size(sz);
                format!("{level_heading}, all files {sz}")
            }
            _ => level_heading.into(),
        };

        format!(
            "{level_heading:width$}",
            width = self.width_chars + self.row_heading_chars
        )
    }

    /// Formats a single parquet file into a string of `width_chars`
    /// characters, which tries to visually depict the timge range of
    /// the file using the width. See docs on [`ParquetFileFormatter`]
    /// for examples.
    fn format_file<P: ParquetFileInfo>(&self, file: &P) -> String {
        // use try_into to force conversion to usize
        let time_width = file.max_time() - file.min_time();

        // special case "zero" width times
        let field_width = if self.min_time == self.max_time {
            self.width_chars
        } else {
            self.time_range_to_chars(time_width)
        }
        // account for starting and ending '|'
        .saturating_sub(2);

        // Get compact display of the file, like 'L0.1'
        // add |--- ---| formatting (based on field width)
        let file_string = format!("|{:-^width$}|", file.display_id(), width = field_width);
        // show indvidual file sizes if they are different
        let show_size = matches!(self.file_size_seen, FileSizeSeen::Many);
        let row_heading = display_format(file, show_size);

        // special case "zero" width times
        if self.min_time == self.max_time {
            return format!(
                "{row_heading:width1$}{file_string:^width2$}",
                width1 = self.row_heading_chars,
                width2 = self.width_chars,
            );
        }

        // otherwise, figure out whitespace padding at start and back
        // based on the relative start time of the file
        // assume time from 0
        let prefix_time_range = file.min_time().saturating_sub(self.min_time);
        let prefix_padding = " ".repeat(self.time_range_to_chars(prefix_time_range));

        // pad the rest with whitespace
        let postfix_padding_len = self
            .width_chars
            .saturating_sub(file_string.len())
            .saturating_sub(prefix_padding.len());
        let postfix_padding = " ".repeat(postfix_padding_len);

        format!(
            "{row_heading:width$}{prefix_padding}{file_string}{postfix_padding}",
            width = self.row_heading_chars
        )
    }
}

fn display_level(compaction_level: &CompactionLevel) -> &'static str {
    match compaction_level {
        CompactionLevel::Initial => "L0",
        CompactionLevel::FileNonOverlapped => "L1",
        CompactionLevel::Final => "L2",
    }
}

/// Format a size for reasonable human reading
pub fn display_size(sz: i64) -> String {
    let kbyte = 1024.0;
    let mbyte = 1024.0 * kbyte;
    let gbyte = 1024.0 * mbyte;

    let sz = sz as f64;
    if sz < kbyte {
        format!("{sz}b")
    } else if sz < mbyte {
        let kb = round(sz / kbyte, 0);
        format!("{kb}kb")
    } else if sz < gbyte {
        let mb = round(sz / mbyte, 0);
        format!("{mb}mb")
    } else {
        let gb = round(sz / gbyte, 2);
        format!("{gb}gb")
    }
}

/// Format a time for reasonable human reading
pub fn display_time(ns: i64) -> String {
    let us = 1000.0;
    let ms = 1000.0 * us;
    let s = 1000.0 * ms;

    let ns = ns as f64;
    if ns < us {
        format!("{ns}ns")
    } else if ns < ms {
        let micro = round(ns / us, 2);
        format!("{micro}us")
    } else if ns < s {
        let milli = round(ns / ms, 2);
        format!("{milli}ms")
    } else {
        let sec = round(ns / s, 2);
        format!("{sec}s")
    }
}

/// return the total file size of all the parquet files
pub fn total_size<P: ParquetFileInfo>(parquet_files: &[P]) -> i64 {
    parquet_files.iter().map(|f| f.file_size_bytes()).sum()
}

// https://stackoverflow.com/questions/28655362/how-does-one-round-a-floating-point-number-to-a-specified-number-of-digits
fn round(x: f64, decimals: u32) -> f64 {
    let y = 10i32.pow(decimals) as f64;
    (x * y).round() / y
}

/// Compact display of level, id min/max time and optional size.
///
/// Example
///
/// ```text
/// L0.1[100,200]@1
/// ```
pub fn display_format<P: ParquetFileInfo>(file: &P, show_size: bool) -> String {
    let file_id = file.display_id();
    let min_time = file.min_time(); // display as i64
    let max_time = file.max_time(); // display as i64
    let sz = file.file_size_bytes();
    let created_at = display_time(file.max_l0_created_at());

    if show_size {
        let sz = display_size(sz);
        format!("{file_id}[{min_time},{max_time}] {created_at} {sz}")
    } else {
        format!("{file_id}[{min_time},{max_time}] {created_at}")
    }
}

#[cfg(test)]
mod test {
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn display_builder() {
        let files = vec![
            ParquetFileBuilder::new(1)
                .with_compaction_level(CompactionLevel::Initial)
                .build(),
            ParquetFileBuilder::new(2)
                .with_compaction_level(CompactionLevel::Initial)
                .build(),
        ];

        insta::assert_yaml_snapshot!(
            format_files("display", &files),
            @r###"
        ---
        - display
        - "L0, all files 1b                                                                                                   "
        - "L0.1[0,0] 0ns            |------------------------------------------L0.1------------------------------------------|"
        - "L0.2[0,0] 0ns            |------------------------------------------L0.2------------------------------------------|"
        "###
        );
    }

    #[test]
    fn display_builder_multi_levels_with_size() {
        let files = vec![
            ParquetFileBuilder::new(1)
                .with_compaction_level(CompactionLevel::Initial)
                .build(),
            ParquetFileBuilder::new(2)
                .with_compaction_level(CompactionLevel::Initial)
                .build(),
            ParquetFileBuilder::new(3)
                .with_compaction_level(CompactionLevel::Final)
                .with_file_size_bytes(42)
                .build(),
        ];

        insta::assert_yaml_snapshot!(
            format_files("display", &files),
            @r###"
        ---
        - display
        - "L0                                                                                                                 "
        - "L0.1[0,0] 0ns 1b         |------------------------------------------L0.1------------------------------------------|"
        - "L0.2[0,0] 0ns 1b         |------------------------------------------L0.2------------------------------------------|"
        - "L2                                                                                                                 "
        - "L2.3[0,0] 0ns 42b        |------------------------------------------L2.3------------------------------------------|"
        "###
        );
    }

    #[test]
    fn display_builder_size_time_ranges() {
        let files = vec![
            ParquetFileBuilder::new(1)
                .with_compaction_level(CompactionLevel::Initial)
                .with_time_range(100, 200)
                .build(),
            ParquetFileBuilder::new(2)
                .with_compaction_level(CompactionLevel::Initial)
                .with_time_range(300, 400)
                .build(),
            // overlapping file
            ParquetFileBuilder::new(11)
                .with_compaction_level(CompactionLevel::Initial)
                .with_time_range(150, 350)
                .with_file_size_bytes(44)
                .build(),
        ];

        insta::assert_yaml_snapshot!(
            format_files("display", &files),
            @r###"
        ---
        - display
        - "L0                                                                                                                 "
        - "L0.1[100,200] 0ns 1b     |------------L0.1------------|                                                            "
        - "L0.2[300,400] 0ns 1b                                                                 |------------L0.2------------|"
        - "L0.11[150,350] 0ns 44b                  |--------------------------L0.11---------------------------|               "
        "###
        );
    }
}
