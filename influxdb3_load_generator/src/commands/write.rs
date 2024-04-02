use crate::commands::common::LoadType;
use crate::line_protocol_generator::{create_generators, Generator};
use crate::report::{SystemStatsReporter, WriteReporter};
use anyhow::Context;
use chrono::{DateTime, Local};
use clap::Parser;
use influxdb3_client::{Client, Precision};
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use super::common::InfluxDb3Config;

#[derive(Debug, Parser)]
#[clap(visible_alias = "w", trailing_var_arg = true)]
pub struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Sampling interval for the writers. They will generate data at this interval and
    /// sleep for the remainder of the interval. Writers stagger writes by this interval divided
    /// by the number of writers.
    #[clap(
        short = 'i',
        long = "interval",
        env = "INFLUXDB3_LOAD_SAMPLING_INTERVAL",
        default_value = "1s"
    )]
    sampling_interval: humantime::Duration,

    /// Number of simultaneous writers. Each writer will generate data at the specified interval.
    #[clap(
        short = 'w',
        long = "writer-count",
        env = "INFLUXDB3_LOAD_WRITERS",
        default_value = "1"
    )]
    writer_count: usize,

    /// Tells the generator to run a single sample for each writer in `writer-count` and output the data to stdout.
    #[clap(long = "dry-run", default_value = "false")]
    dry_run: bool,

    /// The date and time at which to start the timestamps of the generated data.
    ///
    /// Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy
    /// specification like `1 hour` in the past. If not specified, defaults to now.
    #[clap(long, action)]
    start: Option<String>,

    /// The date and time at which to stop the timestamps of the generated data.
    ///
    /// Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy
    /// specification like `1 hour` in the future. If not specified, data will continue generating forever.
    #[clap(long, action)]
    end: Option<String>,
}

pub(crate) async fn command(config: Config) -> Result<(), anyhow::Error> {
    let (client, load_config) = config.influxdb3_config.initialize(LoadType::Write).await?;
    let spec = load_config.write_spec.unwrap();
    let results_file = load_config.write_results_file.unwrap();
    let results_file_path = load_config.write_results_file_path.unwrap();

    println!(
        "creating generators for {} concurrent writers",
        config.writer_count
    );
    let mut generators =
        create_generators(&spec, config.writer_count).context("failed to create generators")?;

    // if dry run is set, output from each generator its id and then a single sample
    if config.dry_run {
        println!("running dry run for each writer\n");
        for g in &mut generators {
            let t = Local::now();
            let dry_run_output = g.dry_run(t.timestamp_millis());
            println!("Writer {}:\n{}", g.writer_id, dry_run_output);
        }
        return Ok(());
    }

    let start_time = if let Some(start_time) = config.start {
        let start_time = parse_time_offset(&start_time, Local::now());
        println!("starting writers from a start time of {:?}. Historical replay will happen as fast as possible until catching up to now or hitting the end time.", start_time);
        Some(start_time)
    } else {
        None
    };

    let end_time = if let Some(end_time) = config.end {
        let end_time = parse_time_offset(&end_time, Local::now());
        println!("ending at {:?}", end_time);
        Some(end_time)
    } else {
        println!(
            "running indefinitely with each writer sending a request every {}",
            config.sampling_interval
        );
        None
    };

    println!("generating results in: {results_file_path}");
    let write_reporter =
        Arc::new(WriteReporter::new(results_file).context("failed to create write reporter")?);

    // blocking task to periodically flush the report to disk
    let reporter = Arc::clone(&write_reporter);
    tokio::task::spawn_blocking(move || {
        reporter.flush_reports();
    });

    // spawn system stats collection
    let stats_reporter = if let (Some(stats_file), Some(stats_file_path)) = (
        load_config.system_stats_file,
        load_config.system_stats_file_path,
    ) {
        println!("generating system stats in: {stats_file_path}");
        let stats_reporter = Arc::new(SystemStatsReporter::new(stats_file)?);
        let s = Arc::clone(&stats_reporter);
        tokio::task::spawn_blocking(move || {
            s.report_stats();
        });
        Some((stats_file_path, stats_reporter))
    } else {
        None
    };

    // spawn tokio tasks for each writer
    let mut tasks = Vec::new();
    for generator in generators {
        let reporter = Arc::clone(&write_reporter);
        let sampling_interval = config.sampling_interval.into();
        let task = tokio::spawn(run_generator(
            generator,
            client.clone(),
            load_config.database_name.clone(),
            reporter,
            sampling_interval,
            start_time,
            end_time,
        ));
        tasks.push(task);
    }

    // wait for all tasks to complete
    for task in tasks {
        task.await?;
    }
    println!("all writers finished");

    write_reporter.shutdown();
    println!("results saved in: {results_file_path}");

    if let Some((stats_file_path, stats_reporter)) = stats_reporter {
        println!("system stats saved in: {stats_file_path}");
        stats_reporter.shutdown();
    }

    Ok(())
}

fn parse_time_offset(s: &str, now: DateTime<Local>) -> DateTime<Local> {
    humantime::parse_rfc3339(s)
        .map(Into::into)
        .unwrap_or_else(|_| {
            let std_duration = humantime::parse_duration(s).expect("Could not parse time");
            let chrono_duration = chrono::Duration::from_std(std_duration)
                .expect("Could not convert std::time::Duration to chrono::Duration");
            now - chrono_duration
        })
}

async fn run_generator(
    mut generator: Generator,
    client: Client,
    database_name: String,
    reporter: Arc<WriteReporter>,
    sampling_interval: Duration,
    start_time: Option<DateTime<Local>>,
    end_time: Option<DateTime<Local>>,
) {
    let mut sample_buffer = vec![];

    // if the start time is set, load the historical samples as quickly as possible
    if let Some(mut start_time) = start_time {
        let mut sample_len = write_sample(
            &mut generator,
            sample_buffer,
            &client,
            &database_name,
            start_time,
            &reporter,
            true,
        )
        .await;

        loop {
            start_time = start_time.add(sampling_interval);
            if start_time > Local::now()
                || end_time
                    .map(|end_time| start_time > end_time)
                    .unwrap_or(false)
            {
                println!(
                    "writer {} finished historical replay at: {:?}",
                    generator.writer_id, start_time
                );
                break;
            }

            sample_buffer = Vec::with_capacity(sample_len);
            sample_len = write_sample(
                &mut generator,
                sample_buffer,
                &client,
                &database_name,
                start_time,
                &reporter,
                false,
            )
            .await;
        }
    }

    // write data until end time or forever
    let mut interval = tokio::time::interval(sampling_interval);
    let mut sample_len = 1024 * 1024 * 1024;

    // we only want to print the error the very first time it happens
    let mut print_err = false;

    loop {
        interval.tick().await;
        let now = Local::now();
        if let Some(end_time) = end_time {
            if now > end_time {
                println!(
                    "writer {} finished writing to end time: {:?}",
                    generator.writer_id, end_time
                );
                return;
            }
        }

        sample_buffer = Vec::with_capacity(sample_len);
        sample_len = write_sample(
            &mut generator,
            sample_buffer,
            &client,
            &database_name,
            now,
            &reporter,
            print_err,
        )
        .await;
        print_err = true;
    }
}

async fn write_sample(
    generator: &mut Generator,
    mut buffer: Vec<u8>,
    client: &Client,
    database_name: &String,
    sample_time: DateTime<Local>,
    reporter: &Arc<WriteReporter>,
    print_err: bool,
) -> usize {
    // generate the sample, and keep track of the length to set the buffer size for the next loop
    let summary = generator
        .write_sample_to(sample_time.timestamp_millis(), &mut buffer)
        .expect("failed to write sample");
    let sample_len = buffer.len();

    // time and send the write request
    let start_request = Instant::now();
    let res = client
        .api_v3_write_lp(database_name)
        .precision(Precision::Millisecond)
        .accept_partial(false)
        .body(buffer)
        .send()
        .await;
    let response_time = start_request.elapsed().as_millis() as u64;

    // log the report
    match res {
        Ok(_) => {
            reporter.report_write(generator.writer_id, summary, response_time, Local::now());
        }
        Err(e) => {
            // if it's the first error, print the details
            if print_err {
                eprintln!(
                    "Error on writer {} writing to server: {:?}",
                    generator.writer_id, e
                );
            }
            reporter.report_failure(generator.writer_id, response_time, Local::now());
        }
    }

    sample_len
}

pub(crate) fn print_help() {
    let built_in_specs = crate::specs::built_in_specs();
    let example = built_in_specs.first().unwrap();
    let mut generators = create_generators(&example.write_spec, 2).unwrap();
    let t = 123;
    let dry_run_output_1 = generators.get_mut(0).unwrap().dry_run(t);
    let dry_run_output_2 = generators.get_mut(1).unwrap().dry_run(t);

    let builtin_help = built_in_specs
        .iter()
        .map(|spec| {
            format!(
                "name: {}\ndescription: {}\n",
                spec.write_spec.name, spec.description
            )
        })
        .collect::<Vec<String>>()
        .join("\n");

    println!(
        r#"You didn't provide a spec path, which is required. For more information about the arguments for this command run:

    influxdb_load_generator write --help

There are some built in specs that you can run just by specifying their name. If you want
to see the JSON for their structure as a starting point, specify their name as the --print-spec
argument. Here's a list of the builtin specs:

{}

Or, if you need a more detailed writeup on specs and how they work here are details about
the example. A spec is just a JSON object specifying how to generate measurements and their
tags and fields. All data will have a millisecond timestamp generated (with that precision
specified) and aligned with the sampling. The generator will run against a single database
and can have many concurrent writers. The spec indicates the shape of the data that should
be generated.

As the generator runs, it will output basic information to stdout. The stats of each
individual request will be written to a results CSV file that you can use after the run to
analyze the performance of write requests to the server.

In the data spec there is an array of measurements. Within each is an array of tags
and an array of fields. Measurements have a name while tags and fields have keys (i.e. tag
key and field key). Tags and fields are scoped to the measurement they are under. If a
tag with key 'foo' appears under two different measurements they are considered different
tags. The same goes for fields. All measurements must have at least 1 field and can have 0
or more tags.

The measurement, tag and field structs have an option called 'copies' which is an integer.
When specified, the data generator will create that many copies of the measurement, tag,
or field and append the copy number to the name/keys. This is useful for generating a large
schema in a test.

Tags have two options that work together that need explanation: cardinality, and
lines_per_sample. Cardinality is the number of unique values that the tag will have.
This cardinality will be split across the number of writers in a test run. Thus if you have
1,000 cardinality and a single writer, the unique values will all get written by that writer.
If you have 1,000 cardinality and 10 writers, each writer will write 100 unique values.

The lines_per_sample option on the measurement is used to control how many of the unique
values are used in a single sampling round. If not specified, all unique values will be used.
This number will be rounded down to the cardinality of the tag with the highest cardinality
for the measurement. This is done on a per writer basis. If you have lines_per_sample of 10
and a tag of 100 cardinality with 1 writer, it will generate 10 lines of that measurement with
each unique tag value going to the next 10 values on the next sample, taking 10 samples to get
through the 100 uniques before it cycles back to the beginning.

Separately, cardinality of tags will be split across the number of writers you have. So if
you have cardinality of 100 and 1 writer, by default it will generate 100 lines of that
measurement with each unique tag value. If you have 10 writers, each writer will generate 10
unique tag values. Thus with 10 writers, the lines_per_sample would max at 10 since each
sample can only generate 10 unique tag values.

The tag spec also has a boolean option called "append_writer_id". Writers are the individual
threads that run and generate and write samples at the same time. The number is set through
the parameter --writer-count. If append_writer_id is set to true, the generator will append
the writer id to the tag value. This is useful for generating unique tag values across
writers, simulating a host id or something similar.

Fields have options for generating static data, or randomly generated data within a range. For
strings, you can specify a static string or a random string of a certain length. Another option
worth noting is the null_probability. This is a float between 0 and 1 that indicates the probability
that a field will be null. If this option is used, you must have another field that does not use
this option (i.e. you must always have at least one field that is guaranteed to have a value).

If you're unsure how an option works or what it will produce, the easiest thing to do is to create
a file and run the generator with the --dry-run option. This will output the data to stdout so you
can see what it looks like before you run it against a server. It will use the --writer-count
value and show what each writer would send in a sample.

The example below shows this functionality generating different kinds of tags and
fields of different value types. First, we show the spec, then we show the output that gets
generated on a dry-run so you can see how the spec translates into generated line protocol.

Here's the spec:

{}

And when run with writer count set to 2, here's what will be sent in a request by each writer.

Writer 1:
{}
Writer 2:
{}"#,
        builtin_help,
        example.write_spec.to_json_string_pretty().unwrap(),
        dry_run_output_1,
        dry_run_output_2
    );
}
