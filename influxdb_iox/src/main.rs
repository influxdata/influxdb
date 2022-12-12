//! Entrypoint of InfluxDB IOx binary
#![recursion_limit = "512"] // required for print_cpu
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use crate::commands::{
    run::all_in_one,
    tracing::{init_logs_and_tracing, init_simple_logs, TroggingGuard},
};
use dotenvy::dotenv;
use influxdb_iox_client::connection::Builder;
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::{debug, warn};
use process_info::VERSION_STRING;
use std::time::Duration;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    str::FromStr,
};
use tokio::runtime::Runtime;

mod commands {
    pub mod catalog;
    pub mod compactor;
    pub mod debug;
    pub mod import;
    pub mod namespace;
    pub mod query;
    pub mod query_ingester;
    pub mod remote;
    pub mod run;
    pub mod sql;
    pub mod storage;
    pub mod tracing;
    pub mod write;
}

mod process_info;

enum ReturnCode {
    Failure = 1,
}

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
compile_error!("heappy and jemalloc_replacing_malloc features are mutually exclusive");

#[derive(Debug, clap::Parser)]
#[clap(
    name = "influxdb_iox",
    version = &VERSION_STRING[..],
    disable_help_flag = true,
    arg(
        clap::Arg::new("help")
            .long("help")
            .help("Print help information")
            .action(clap::ArgAction::Help)
            .global(true)
    ),
    about = "InfluxDB IOx server and command line tools",
    long_about = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server in all-in-one "run" mode
    influxdb_iox

    # Display all available modes, including "run"
    influxdb_iox --help

    # Run the InfluxDB IOx server in all-in-one mode with extra verbose logging
    influxdb_iox -v

    # Run InfluxDB IOx with full debug logging specified with LOG_FILTER
    LOG_FILTER=debug influxdb_iox

    # Display all "run" mode settings
    influxdb_iox run --help

    # Run the interactive SQL prompt
    influxdb_iox sql

Command are generally structured in the form:
    <type of object> <action> <arguments>

For example, a command such as the following shows all actions
    available for namespaces, including `list` and `retention`.

    influxdb_iox namespace --help
"#
)]
struct Config {
    /// gRPC address of IOx server to connect to
    #[clap(
        short,
        long,
        global = true,
        env = "IOX_ADDR",
        default_value = "http://127.0.0.1:8082",
        action
    )]
    host: String,

    /// Additional headers to add to CLI requests
    ///
    /// Values should be key value pairs separated by ':'
    #[clap(long, global = true, action)]
    header: Vec<KeyValue<http::header::HeaderName, http::HeaderValue>>,

    /// Configure the request timeout for CLI requests
    #[clap(
        long,
        global = true,
        default_value = "30s",
        value_parser = humantime::parse_duration,
    )]
    rpc_timeout: Duration,

    /// Automatically generate an uber-trace-id header for CLI requests
    ///
    /// The generated trace ID will be emitted at the beginning of the response.
    #[clap(long, global = true, action)]
    gen_trace_id: bool,

    /// Add an InfluxDB Cloud style authorization header with the specified token
    ///
    /// This is shorthand for adding a header of the form
    /// `Authorization: Token <token>`
    #[clap(long, global = true, env = "INFLUX_TOKEN", action)]
    token: Option<String>,

    /// Set the maximum number of threads to use. Defaults to the number of
    /// cores on the system
    #[clap(long, action)]
    num_threads: Option<usize>,

    /// Supports having all-in-one be the default command.
    #[clap(flatten)]
    all_in_one_config: all_in_one::Config,

    #[clap(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Run the InfluxDB IOx server
    // Clippy recommended boxing this variant because it's much larger than the others
    Run(Box<commands::run::Config>),

    /// Commands to run against remote IOx APIs
    Remote(commands::remote::Config),

    /// Start IOx interactive SQL REPL loop
    Sql(commands::sql::Config),

    /// Various commands for catalog manipulation
    Catalog(commands::catalog::Config),

    /// Various commands for compactor manipulation
    Compactor(Box<commands::compactor::Config>),

    /// Interrogate internal data
    Debug(commands::debug::Config),

    /// Initiate a read request to the gRPC storage service.
    Storage(commands::storage::Config),

    /// Write data into the specified namespace
    Write(commands::write::Config),

    /// Query the data with SQL
    Query(commands::query::Config),

    /// Query the ingester only
    QueryIngester(commands::query_ingester::Config),

    /// Commands related to the bulk ingest of data
    Import(commands::import::Config),

    /// Various commands for namespace manipulation
    Namespace(commands::namespace::Config),
}

fn main() -> Result<(), std::io::Error> {
    install_crash_handler(); // attempt to render a useful stacktrace to stderr

    // load all environment variables from .env before doing anything
    load_dotenv();

    let config: Config = clap::Parser::parse();

    let tokio_runtime = get_runtime(config.num_threads)?;
    tokio_runtime.block_on(async move {
        let host = config.host;
        let headers = config.header;
        let log_verbose_count = config.all_in_one_config.logging_config.log_verbose_count;
        let rpc_timeout = config.rpc_timeout;

        let connection = || async move {
            let mut builder = headers.into_iter().fold(Builder::default(), |builder, kv| {
                debug!(name=?kv.key, value=?kv.value, "Setting header");
                builder.header(kv.key, kv.value)
            });

            builder = builder.timeout(rpc_timeout);

            if config.gen_trace_id {
                let key = http::header::HeaderName::from_str(
                    trace_exporters::DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME,
                )
                .unwrap();
                let trace_id = gen_trace_id();
                let value = http::header::HeaderValue::from_str(trace_id.as_str()).unwrap();
                debug!(name=?key, value=?value, "Setting trace header");
                builder = builder.header(key, value);

                // Emit trace id information
                println!("Trace ID set to {}", trace_id);
            }

            if let Some(token) = config.token.as_ref() {
                let key = http::header::HeaderName::from_str("Authorization").unwrap();
                let value =
                    http::header::HeaderValue::from_str(&format!("Token {}", token)).unwrap();
                debug!(name=?key, value=?value, "Setting token header");
                builder = builder.header(key, value);
            }

            match builder.build(&host).await {
                Ok(connection) => connection,
                Err(e) => {
                    eprintln!("Error connecting to {}: {}", host, e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        };

        fn handle_init_logs(r: Result<TroggingGuard, trogging::Error>) -> TroggingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }

        match config.command {
            None => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = all_in_one::command(config.all_in_one_config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Remote(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::remote::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Run(config)) => {
                let _tracing_guard =
                    handle_init_logs(init_logs_and_tracing(log_verbose_count, &config));
                if let Err(e) = commands::run::command(*config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Sql(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::sql::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Storage(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::storage::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Catalog(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::catalog::command(config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Compactor(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::compactor::command(*config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Debug(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::debug::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Write(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::write::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Query(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::query::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::QueryIngester(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::query_ingester::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Import(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::import::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Namespace(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::namespace::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        }
    });

    Ok(())
}

// Generates a compatible header values for a jaeger trace context header.
fn gen_trace_id() -> String {
    let now = SystemProvider::new().now();
    let mut hasher = DefaultHasher::new();
    now.timestamp_nanos().hash(&mut hasher);

    format!("{:x}:1112223334445:0:1", hasher.finish())
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<usize>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );

            match num_threads {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(num_threads)
                    .build(),
            }
        }
    }
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

// Based on ideas from
// https://github.com/servo/servo/blob/f03ddf6c6c6e94e799ab2a3a89660aea4a01da6f/ports/servo/main.rs#L58-L79
fn install_crash_handler() {
    unsafe {
        set_signal_handler(libc::SIGSEGV, signal_handler); // handle segfaults
        set_signal_handler(libc::SIGILL, signal_handler); // handle stack overflow and unsupported CPUs
        set_signal_handler(libc::SIGBUS, signal_handler); // handle invalid memory access
    }
}

unsafe extern "C" fn signal_handler(sig: i32) {
    use backtrace::Backtrace;
    use std::process::abort;
    let name = std::thread::current()
        .name()
        .map(|n| format!(" for thread \"{}\"", n))
        .unwrap_or_else(|| "".to_owned());
    eprintln!(
        "Signal {}, Stack trace{}\n{:?}",
        sig,
        name,
        Backtrace::new()
    );
    abort();
}

// based on https://github.com/adjivas/sig/blob/master/src/lib.rs#L34-L52
unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = std::mem::zeroed();

    // Block all signals during the handler. This is the expected behavior, but
    // it's not guaranteed by `signal()`.
    if sigfillset(&mut sigset) != -1 {
        // Done because sigaction has private members.
        // This is safe because sa_restorer and sa_handlers are pointers that
        // might be null (that is, zero).
        let mut action: sigaction = std::mem::zeroed();

        // action.sa_flags = 0;
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;

        sigaction(signal, &action, std::ptr::null_mut());
    }
}

/// A ':' separated key value pair
#[derive(Debug, Clone)]
struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> std::str::FromStr for KeyValue<K, V>
where
    K: FromStr,
    V: FromStr,
    K::Err: std::fmt::Display,
    V::Err: std::fmt::Display,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use itertools::Itertools;
        match s.splitn(2, ':').collect_tuple() {
            Some((key, value)) => {
                let key = K::from_str(key).map_err(|e| e.to_string())?;
                let value = V::from_str(value).map_err(|e| e.to_string())?;
                Ok(Self { key, value })
            }
            None => Err(format!(
                "Invalid key value pair - expected 'KEY:VALUE' got '{}'",
                s
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    #[test]
    // ensures that dependabot doesn't update dotenvy until https://github.com/allan2/dotenvy/issues/12 is fixed
    fn dotenvy_regression() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "# '").unwrap();
        dotenvy::from_path(tmp.path()).unwrap();
    }
}
