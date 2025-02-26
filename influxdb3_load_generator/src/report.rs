//! Trackers and report generators for write and query runs

use crate::line_protocol_generator::{WriteSummary, WriterId};
use crate::query_generator::QuerierId;
use anyhow::{Context, bail};
use chrono::{DateTime, Local};
use influxdb3_process::INFLUXDB3_PROCESS_NAME;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::time::{Duration, Instant};
use sysinfo::{Pid, Process, ProcessRefreshKind, System};
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
    csv_writer: Mutex<csv::Writer<File>>,
    shutdown: Mutex<bool>,
}

impl WriteReporter {
    pub fn new(csv_file: File) -> Result<Self, anyhow::Error> {
        // open csv file for writing
        let mut csv_writer = csv::Writer::from_writer(csv_file);
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

            if *self.shutdown.lock() {
                return;
            }

            if console_stats.last_console_output_time.elapsed() > CONSOLE_REPORT_INTERVAL {
                let elapsed_millis = console_stats.last_console_output_time.elapsed().as_millis();

                println!(
                    "write: success: {:.0}/s, error: {:.0}/s, lines: {:.0}/s, bytes: {:.0}/s",
                    console_stats.success as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.error as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.lines as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.bytes as f64 / elapsed_millis as f64 * 1000.0,
                );

                console_stats = ConsoleReportStats::new();
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

#[derive(Copy, Clone, Debug)]
pub struct QuerierReport {
    query_instant: Instant,
    wall_time: DateTime<Local>,
    response_time_ms: u64,
    response_status: u16,
    rows_returned: u64,
    querier_id: QuerierId,
}

#[derive(Debug)]
pub struct QueryReporter {
    state: Mutex<Vec<QuerierReport>>,
    csv_writer: Mutex<csv::Writer<File>>,
    shutdown: Mutex<bool>,
}

impl QueryReporter {
    pub fn new(csv_file: File) -> Self {
        let csv_writer = Mutex::new(csv::Writer::from_writer(csv_file));
        Self {
            state: Mutex::new(vec![]),
            csv_writer,
            shutdown: Mutex::new(false),
        }
    }

    pub fn report(
        &self,
        querier_id: QuerierId,
        response_status: u16,
        response_time_ms: u64,
        rows_returned: u64,
        wall_time: DateTime<Local>,
    ) {
        let mut state = self.state.lock();
        state.push(QuerierReport {
            query_instant: Instant::now(),
            wall_time,
            response_time_ms,
            response_status,
            rows_returned,
            querier_id,
        })
    }

    pub fn flush_reports(&self) {
        let start_time = Instant::now();
        let mut console_stats = QueryConsoleStats::new();

        loop {
            let reports = {
                let mut state = self.state.lock();
                let mut reports = Vec::with_capacity(state.len());
                std::mem::swap(&mut reports, &mut *state);
                reports
            };

            let mut csv_writer = self.csv_writer.lock();
            for report in reports {
                let test_time_ms = report.query_instant.duration_since(start_time).as_millis();
                if report.response_status > 199 && report.response_status < 300 {
                    console_stats.success += 1;
                } else {
                    console_stats.error += 1;
                }
                console_stats.rows += report.rows_returned;
                csv_writer
                    .serialize(QueryRecord {
                        test_time_ms,
                        response_ms: report.response_time_ms,
                        response_status: report.response_status,
                        rows: report.rows_returned,
                        querier_id: report.querier_id,
                        wall_time: report.wall_time,
                    })
                    .expect("failed to write csv report record");
            }
            csv_writer.flush().expect("failed to flush csv reports");

            if *self.shutdown.lock() {
                return;
            }

            if console_stats.last_console_output_time.elapsed() > CONSOLE_REPORT_INTERVAL {
                let elapsed_millis = console_stats.last_console_output_time.elapsed().as_millis();

                println!(
                    "query: success: {:.0}/s, error: {:.0}/s, rows: {:.0}/s",
                    console_stats.success as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.error as f64 / elapsed_millis as f64 * 1000.0,
                    console_stats.rows as f64 / elapsed_millis as f64 * 1000.0,
                );

                console_stats = QueryConsoleStats::new();
            }

            std::thread::sleep(REPORT_FLUSH_INTERVAL);
        }
    }

    pub fn shutdown(&self) {
        *self.shutdown.lock() = true;
    }
}

#[derive(Debug, Serialize)]
struct QueryRecord {
    querier_id: QuerierId,
    wall_time: DateTime<Local>,
    test_time_ms: u128,
    response_ms: u64,
    response_status: u16,
    rows: u64,
}

struct QueryConsoleStats {
    last_console_output_time: Instant,
    success: usize,
    error: usize,
    rows: u64,
}

impl QueryConsoleStats {
    fn new() -> Self {
        Self {
            last_console_output_time: Instant::now(),
            success: 0,
            error: 0,
            rows: 0,
        }
    }
}

const SYSTEM_STATS_REPORT_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Copy, Clone, Serialize)]
pub struct SystemStatsRecord {
    wall_time: DateTime<Local>,
    test_time_ms: u128,
    cpu_usage: f32,
    written_bytes: u64,
    read_bytes: u64,
    memory_bytes: u64,
    virtual_memory_bytes: u64,
}

#[derive(Debug)]
pub struct SystemStatsReporter {
    pid: Pid,
    system: Mutex<System>,
    csv_writer: Mutex<csv::Writer<File>>,
    shutdown: Mutex<bool>,
}

impl SystemStatsReporter {
    pub fn new(csv_file: File) -> Result<Self, anyhow::Error> {
        let csv_writer = Mutex::new(csv::Writer::from_writer(csv_file));
        let mut system = System::new_all();
        let mut processes = system
            .processes_by_exact_name(INFLUXDB3_PROCESS_NAME)
            .collect::<Vec<&Process>>();
        if processes.is_empty() {
            bail!("there is no '{}' process", INFLUXDB3_PROCESS_NAME);
        }
        if processes.len() > 1 {
            bail!(
                "ensure there is only one '{}' process running on your operating system",
                INFLUXDB3_PROCESS_NAME
            );
        }
        let pid = processes.pop().unwrap().pid();
        // refresh the system stats for the process to initialize the baseline:
        system.refresh_pids(&[pid]);
        Ok(Self {
            pid,
            system: Mutex::new(system),
            csv_writer,
            shutdown: Mutex::new(false),
        })
    }

    pub fn report_stats(&self) {
        let start_time = Instant::now();

        loop {
            let mut system = self.system.lock();
            system.refresh_pids_specifics(
                &[self.pid],
                ProcessRefreshKind::new()
                    .with_cpu()
                    .with_memory()
                    .with_disk_usage(),
            );
            let process = system
                .process(self.pid)
                .unwrap_or_else(|| panic!("process with pid: {}", self.pid));
            let mut csv_writer = self.csv_writer.lock();
            let test_time_ms = Instant::now().duration_since(start_time).as_millis();
            csv_writer
                .serialize(SystemStatsRecord {
                    wall_time: Local::now(),
                    test_time_ms,
                    cpu_usage: process.cpu_usage(),
                    written_bytes: process.disk_usage().written_bytes,
                    read_bytes: process.disk_usage().read_bytes,
                    memory_bytes: process.memory(),
                    virtual_memory_bytes: process.virtual_memory(),
                })
                .expect("failed to write csv record for system stats");
            csv_writer.flush().expect("flush system stats csv reports");

            if *self.shutdown.lock() {
                return;
            }

            std::thread::sleep(
                sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.max(SYSTEM_STATS_REPORT_INTERVAL),
            );
        }
    }

    pub fn shutdown(&self) {
        *self.shutdown.lock() = true;
    }
}
