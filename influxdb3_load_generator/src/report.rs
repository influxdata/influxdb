//! Trackers and report generators for write and query runs

use crate::line_protocol_generator::{WriteSummary, WriterId};
use anyhow::Context;
use chrono::{DateTime, Local};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};
// Logged reports will be flushed to the csv file on this interval
const REPORT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

const CONSOLE_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy)]
pub struct WriterReport {
    summary: Option<WriteSummary>, // failed write if none
    write_instant: Instant,
    wall_time: DateTime<Local>,
    response_time_ms: u64,
    writer_id: usize,
}

#[derive(Debug)]
pub struct WriteReporter {
    state: Mutex<Vec<WriterReport>>,
    csv_writer: Mutex<csv::Writer<std::fs::File>>,
    shutdown: Mutex<bool>,
}

impl WriteReporter {
    pub fn new(csv_filename: &str) -> Result<Self, anyhow::Error> {
        // open csv file for writing
        let mut csv_writer = csv::Writer::from_path(csv_filename)?;
        // write header
        csv_writer
            .write_record([
                "writer_id",
                "response",
                "latency_ms",
                "test_time_ms",
                "sample_number",
                "bytes",
                "lines",
                "tags",
                "fields",
                "wall_time",
            ])
            .context("failed to write csv report header")?;

        Ok(Self {
            state: Mutex::new(Vec::new()),
            csv_writer: Mutex::new(csv_writer),
            shutdown: Mutex::new(false),
        })
    }

    pub fn report_failure(
        &self,
        writer_id: usize,
        response_time_ms: u64,
        wall_time: DateTime<Local>,
    ) {
        let mut state = self.state.lock();
        state.push(WriterReport {
            summary: None,
            write_instant: Instant::now(),
            wall_time,
            response_time_ms,
            writer_id,
        });
    }

    pub fn report_write(
        &self,
        writer_id: usize,
        summary: WriteSummary,
        response_time_ms: u64,
        wall_time: DateTime<Local>,
    ) {
        let mut state = self.state.lock();
        state.push(WriterReport {
            summary: Some(summary),
            write_instant: Instant::now(),
            wall_time,
            response_time_ms,
            writer_id,
        });
    }

    /// Run in a spawn blocking task to flush reports to the csv file
    pub fn flush_reports(&self) {
        let start_time = Instant::now();
        let mut sample_counts: HashMap<WriterId, usize> = HashMap::new();
        let mut console_stats = ConsoleReportStats::new();

        loop {
            let reports = {
                let mut state = self.state.lock();
                let mut reports = Vec::with_capacity(state.len());
                std::mem::swap(&mut reports, &mut *state);
                reports
            };

            let mut csv_writer = self.csv_writer.lock();
            for report in reports {
                let test_time = report.write_instant.duration_since(start_time).as_millis();
                let sample_number = sample_counts.entry(report.writer_id).or_insert(0);
                *sample_number += 1;

                if let Some(summary) = report.summary {
                    csv_writer
                        .write_record(&[
                            report.writer_id.to_string(),
                            "200".to_string(),
                            report.response_time_ms.to_string(),
                            test_time.to_string(),
                            sample_number.to_string(),
                            summary.bytes_written.to_string(),
                            summary.lines_written.to_string(),
                            summary.tags_written.to_string(),
                            summary.fields_written.to_string(),
                            report.wall_time.to_string(),
                        ])
                        .expect("failed to write csv report record");

                    console_stats.success += 1;
                    console_stats.lines += summary.lines_written;
                    console_stats.bytes += summary.bytes_written;
                } else {
                    csv_writer
                        .write_record(&[
                            report.writer_id.to_string(),
                            "500".to_string(),
                            report.response_time_ms.to_string(),
                            test_time.to_string(),
                            sample_number.to_string(),
                            "0".to_string(),
                            "0".to_string(),
                            "0".to_string(),
                            "0".to_string(),
                            report.wall_time.to_string(),
                        ])
                        .expect("failed to write csv report record");

                    console_stats.error += 1;
                }
            }

            csv_writer.flush().expect("failed to flush csv reports");

            if console_stats.last_console_output_time.elapsed() > CONSOLE_REPORT_INTERVAL {
                let elapsed_millis = console_stats.last_console_output_time.elapsed().as_millis();

                println!(
                    "success: {:.0}/s, error: {:.0}/s, lines: {:.0}/s, bytes: {:.0}/s",
                    console_stats.success as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.error as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.lines as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.bytes as f64 / elapsed_millis as f64 * 1000.0,
                );

                console_stats = ConsoleReportStats::new();
            }

            if *self.shutdown.lock() {
                return;
            }

            std::thread::sleep(REPORT_FLUSH_INTERVAL);
        }
    }

    pub fn shutdown(&self) {
        *self.shutdown.lock() = true;
    }
}

struct ConsoleReportStats {
    last_console_output_time: Instant,
    success: usize,
    error: usize,
    lines: usize,
    bytes: usize,
}

impl ConsoleReportStats {
    fn new() -> Self {
        Self {
            last_console_output_time: Instant::now(),
            success: 0,
            error: 0,
            lines: 0,
            bytes: 0,
        }
    }
}
