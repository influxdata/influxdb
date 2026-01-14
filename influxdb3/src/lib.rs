//! Entrypoint of the influxdb3 binary
#![recursion_limit = "512"] // required for print_cpu
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
missing_debug_implementations,
clippy::explicit_iter_loop,
clippy::use_self,
clippy::clone_on_ref_ptr,
// See https://github.com/influxdata/influxdb_iox/pull/1671
clippy::future_not_send
)]
#![allow(unused_crate_dependencies)]

use clap::{CommandFactory, FromArgMatches, parser::ValueSource};
use dotenvy::dotenv;
use influxdb3_clap_blocks::tokio::{TokioDatafusionConfig, TokioIoConfig};
use influxdb3_process::VERSION_STRING;
use influxdb3_telemetry::ServeInvocationMethod;
use observability_deps::tracing::warn;
use owo_colors::OwoColorize;
use std::collections::HashMap;

use trogging::{
    TroggingGuard,
    cli::LoggingConfigBuilderExt,
    tracing_subscriber::{Registry, prelude::*},
};

pub mod commands {
    pub mod common;
    pub mod create;
    pub mod delete;
    pub mod disable;
    pub mod enable;
    pub mod helpers;
    pub mod install;
    pub mod query;
    pub mod serve;
    pub mod show;
    pub mod test;
    pub mod update;
    pub mod write;
}

mod env_compat;

enum ReturnCode {
    Failure = 1,
}

#[derive(Debug, clap::Parser)]
#[clap(
name = "influxdb3",
version = &VERSION_STRING[..],
disable_help_flag = true,
disable_help_subcommand = true,
arg(
clap::Arg::new("help")
.short('h')
.long("help")
.help("Print help information")
.action(clap::ArgAction::HelpShort)
.global(true)
),
arg(
clap::Arg::new("help-all")
.long("help-all")
.help("Print more detailed help information")
.action(clap::ArgAction::HelpLong)
.global(true)
),
about = "InfluxDB 3 Core server and command line tools",
long_about = r#"InfluxDB 3 Core server and command line tools

Examples:
    # Run the InfluxDB 3 Core server
    influxdb3 serve --object-store file --data-dir ~/.influxdb3 --node-id my_node_name

    # Display all commands short form
    influxdb3 -h

    # Display all commands long form
    influxdb3 --help

    # Run the InfluxDB 3 Core server with extra verbose logging
    influxdb3 serve -v --object-store file --data-dir ~/.influxdb3 --node-id my_node_name

    # Run InfluxDB 3 Core with full debug logging specified with LOG_FILTER
    LOG_FILTER=debug influxdb3 serve --object-store file --data-dir ~/.influxdb3 --node-id my_node_name
"#
)]
struct Config {
    #[clap(flatten)]
    runtime_config: TokioIoConfig,

    #[clap(subcommand)]
    command: Option<Command>,
}

// Ignoring clippy here since this enum is just used for running
// the CLI command
#[derive(Debug, clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Enable a resource such as a trigger
    Enable(commands::enable::Config),

    /// Create a resource such as a database or auth token
    Create(commands::create::Config),

    /// Disable a resource such as a trigger
    Disable(commands::disable::Config),

    /// Delete a resource such as a database or table
    Delete(commands::delete::Config),

    /// Perform a query against a running InfluxDB 3 Core server
    Query(commands::query::Config),

    /// Run the InfluxDB 3 Core server
    Serve(commands::serve::Config),

    /// Install packages for the processing engine
    Install(commands::install::Config),

    /// List resources on the InfluxDB 3 Core server
    Show(commands::show::Config),

    /// Test things, such as plugins, work the way you expect
    Test(commands::test::Config),

    /// Update resources on the InfluxDB 3 Core server
    Update(commands::update::Config),
    /// Perform a set of writes to a running InfluxDB 3 Core server
    Write(commands::write::Config),
}

impl Command {
    fn serve_name() -> String {
        // I'd like this to be programmatically derived from Serve variant above without
        // needing to make the Config value...
        "serve".to_string()
    }
}

pub fn startup(args: Vec<String>) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    install_crash_handler(); // attempt to render a useful stacktrace to stderr

    // load all environment variables from .env before doing anything
    load_dotenv();

    // Copy deprecated environment variable aliases for backwards compatibility.
    // Must be called BEFORE clap parsing so that old env var names still work.
    env_compat::copy_deprecated_env_aliases();

    // Handle printing help messages for each command so that we can have a custom
    // output with both a help and help-all message. We have to disable the help
    // flag and manually parse the os args here to check for both if the help flags
    // are present and for the subcommand itself. This is all because the
    // templating language for the help messages in clap is incredibly sparse and
    // impractical to use. Therefore we have to sacrifice ease of maintainability
    // for a more practical user experience.
    //
    // We must check for the help flags first else clap will complain if we do not
    // have certain args set when we call `Config::parse()` f.ex `influxdb3 serve -h` will fail with our current derive as `--node-id` is required. This would be confusing as many users will expect to just be able to pass `-h` and get some help spat out. The joys of manually implementing `-h/--help/--help-all`
    maybe_print_help();

    // Copy deprecated environment variables
    TokioIoConfig::copy_deprecated_env_aliases();
    TokioDatafusionConfig::copy_deprecated_env_aliases();

    // Note the help code above *must* run before this function call
    let matches = Config::command().get_matches_from(args.clone());

    let config: Config = match Config::from_arg_matches(&matches) {
        Ok(config) => config,
        Err(e) => {
            // Format the error with command context to include usage information
            let mut cmd = Config::command();
            let err = e.format(&mut cmd);
            err.exit();
        }
    };

    let Config {
        command,
        runtime_config,
    } = config;

    match command {
        Some(Command::Serve(serve_config)) => serve_main(serve_config, &matches, runtime_config),
        None => {
            // special handling for no subcommand at all
            //
            // in this case, default to Command::Serve, with a set of defaults are not
            // normal defaults for when the serve subcommand has been explicitly used, but
            // we need to be mindful that env vars _might_ be set that apply once we add the serve
            // subcommand so we cannot specify more flags until we know env vars aren't set.
            // By definition there's no serve config, we add our own args and reparse.
            // We could build a commands::serve::Config directly but then we have to specify
            // every field of every struct in the config.

            let mut args_with_serve = args.clone();
            args_with_serve.push(Command::serve_name());

            // There is one required param that we need to be set --node-id; the rest
            // of the serve flags have defaults defined in the derive statements.
            // The error from clap is MissingRequiredArgument but doesn't say which

            // hostname should be sufficiently constant for multiple runs
            let hostname = get_hostname_or_primary();
            let hostname_node_id = format!("{}-node", hostname);

            let push_node_id = |mut v: Vec<String>, hostname: String| -> Vec<String> {
                let node_id = format!("{}-node", hostname);
                v.push("--node-id".to_string());
                v.push(node_id);
                v
            };

            type FlagCaseActions = Vec<fn(Vec<String>, String) -> Vec<String>>;
            let cases: Vec<FlagCaseActions> = vec![
                // order of these matters because node id can come from an env var
                vec![],             // node id provided via env vars
                vec![push_node_id], // no node id provided
            ];

            // explicitly indicate that we're starting with implied serve subcommand
            args_with_serve.push("--serve-invocation-method".to_string());
            args_with_serve.push(ServeInvocationMethod::QuickStart.to_string());

            let mut matches: Option<clap_builder::ArgMatches> = None;
            for (i, case) in cases.iter().enumerate() {
                let mut args = args_with_serve.clone();
                for f in case {
                    args = f(args, hostname.clone());
                }
                matches = Some(match Config::command().try_get_matches_from(args.clone()) {
                    Ok(m) => m,
                    Err(err)
                        if err.kind() == clap::error::ErrorKind::MissingRequiredArgument
                            && i != cases.len() - 1 =>
                    {
                        // an error and we're not on the last attempt!
                        continue;
                    }
                    Err(err) => {
                        let mut cmd = Config::command();
                        let err = err.format(&mut cmd);
                        err.exit();
                    }
                });
                break;
            }

            let matches = matches.unwrap(); // guaranteed
            let config: Config = match Config::from_arg_matches(&matches) {
                Ok(config) => config,
                Err(e) => {
                    // should be unreachable
                    let mut cmd = Config::command();
                    let err = e.format(&mut cmd);
                    err.exit();
                }
            };

            if let Some(Command::Serve(serve_config)) = config.command {
                let configured_node_id = match serve_config.node_id.get_node_id() {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("Serve command failed: {e}\n");
                        std::process::exit(ReturnCode::Failure as _)
                    }
                };

                if configured_node_id == hostname_node_id {
                    eprintln!(
                        "Using auto-generated node id: {}. For production deployments, explicitly set --node-id",
                        hostname_node_id
                    );
                }
                serve_main(serve_config, &matches, config.runtime_config)
            } else {
                unreachable!("unreachable because we set the serve command explicitly")
            }
        }
        other => non_serve_main(other, runtime_config),
    }
}

/// Get the system hostname, falling back to "primary" if unavailable
fn get_hostname_or_primary() -> String {
    hostname::get()
        .ok()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or("primary".to_string())
}

fn serve_main(
    serve_config: commands::serve::Config,
    matches: &clap::ArgMatches,
    runtime_config: TokioIoConfig,
) -> Result<(), std::io::Error> {
    // Extract user-provided parameters only for serve command
    let user_params = extract_user_params(matches);

    let tokio_runtime = runtime_config.builder()?.build()?;
    if let Err(e) = tokio_runtime.block_on(async move {
        fn handle_init_logs(r: Result<TroggingGuard, trogging::Error>) -> TroggingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {e}");
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }
        let _tracing_guard = handle_init_logs(init_logs_and_tracing(&serve_config.logging_config));

        commands::serve::command(serve_config, user_params).await
    }) {
        eprintln!("Serve command failed: {e}");

        std::process::exit(ReturnCode::Failure as _)
    }
    Ok(())
}

fn non_serve_main(
    command: Option<Command>,
    runtime_config: TokioIoConfig,
) -> Result<(), std::io::Error> {
    let tokio_runtime = runtime_config.builder()?.build()?;

    tokio_runtime.block_on(async move {
        match command {
            None => eprintln!("command required, -h/--help/--help-all for help"),
            Some(Command::Enable(config)) => {
                if let Err(e) = commands::enable::command(config).await {
                    eprintln!("Enable command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Create(config)) => {
                if let Err(e) = commands::create::command(config).await {
                    eprintln!("Create command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Disable(config)) => {
                if let Err(e) = commands::disable::command(config).await {
                    eprintln!("Disable command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Delete(config)) => {
                if let Err(e) = commands::delete::command(config).await {
                    eprintln!("Delete command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Serve(_)) => {
                unreachable!("serve command must be handled in a different tokio runtime")
            }
            Some(Command::Install(config)) => {
                if let Err(e) = commands::install::command(config).await {
                    eprintln!("Install command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Show(config)) => {
                if let Err(e) = commands::show::command(config).await {
                    eprintln!("Show command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Test(config)) => {
                if let Err(e) = commands::test::command(config).await {
                    eprintln!("Test command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Update(config)) => {
                if let Err(e) = commands::update::command(config).await {
                    eprintln!("Update command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Query(config)) => {
                if let Err(e) = commands::query::command(config).await {
                    eprintln!("Query command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Write(config)) => {
                if let Err(e) = commands::write::command(config).await {
                    eprintln!("Write command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        }
    });

    Ok(())
}

/// Print the help for the cli if asked for and then exit the program
fn maybe_print_help() {
    #[allow(clippy::if_same_then_else)] // They are in fact dear reader not the same
    let mut help = false;
    let mut help_all = false;
    let mut command = None;
    enum SubCommand {
        Enable,
        Create,
        Disable,
        Delete,
        Query,
        Serve,
        Install,
        Show,
        Test,
        Write,
    }

    // Parse the args to see if we have any of the help flags available and which
    // subcommand if it exists
    for arg in std::env::args_os() {
        let arg = arg.into_string().unwrap_or_default();
        // Only check for the help flags if a command is set
        if command.is_some() {
            match arg.as_str() {
                "-h" | "--help" => help = true,
                "--help-all" => help_all = true,
                _ => continue,
            }
        } else {
            match arg.as_str() {
                "-h" | "--help" => help = true,
                "--help-all" => help_all = true,
                "enable" => command = Some(SubCommand::Enable),
                "create" => command = Some(SubCommand::Create),
                "disable" => command = Some(SubCommand::Disable),
                "delete" => command = Some(SubCommand::Delete),
                "query" => command = Some(SubCommand::Query),
                "serve" => command = Some(SubCommand::Serve),
                "install" => command = Some(SubCommand::Install),
                "show" => command = Some(SubCommand::Show),
                "test" => command = Some(SubCommand::Test),
                "write" => command = Some(SubCommand::Write),
                _ => continue,
            }
        }
    }
    if help {
        match command {
            None => {
                println!(
                    include_str!("help/influxdb3.txt"),
                    "Usage:".bold().underline(),
                    "influxdb3".bold(),
                    "Common Commands:".bold().underline(),
                    "serve".bold(),
                    "query, q".bold(),
                    "write, w".bold(),
                    "update".bold(),
                    "Resource Management:".bold().underline(),
                    "create".bold(),
                    "show".bold(),
                    "delete".bold(),
                    "enable".bold(),
                    "disable".bold(),
                    "System Management:".bold().underline(),
                    "install".bold(),
                    "test".bold(),
                    "Common Options:".bold().underline(),
                    "Advanced Help Options:".bold().underline(),
                );
                std::process::exit(0);
            }
            // Some(SubCommand::Enable) => println!(include_str!("help/enable.txt")),
            // Some(SubCommand::Create) => println!(include_str!("help/create.txt")),
            // Some(SubCommand::Disable) => println!(include_str!("help/disable.txt")),
            // Some(SubCommand::Delete) => println!(include_str!("help/delete.txt")),
            Some(SubCommand::Serve) => {
                println!(
                    include_str!("help/serve.txt"),
                    "Usage: influxdb3 serve".bold(),
                    "Required:".bold().underline(),
                    "Common Options:".bold().underline(),
                    "Storage Options:".bold().underline(),
                    "AWS S3 Storage:".bold().underline(),
                    "Google Cloud Storage:".bold().underline(),
                    "Azure Blob Storage:".bold().underline(),
                    "Processing Engine Options:".bold().underline(),
                    "Additional Options:".bold().underline(),
                );
                std::process::exit(0);
            }
            // Some(SubCommand::Install) => println!(include_str!("help/install.txt")),
            // Some(SubCommand::Show) => println!(include_str!("help/show.txt")),
            // Some(SubCommand::Test) => println!(include_str!("help/test.txt")),
            // Some(SubCommand::Query) => println!(include_str!("help/query.txt")),
            // Some(SubCommand::Write) => println!(include_str!("help/write.txt")),
            _ => {}
        }
    } else if help_all {
        match command {
            None => {
                println!(
                    include_str!("help/influxdb3_all.txt"),
                    "Usage:".bold().underline(),
                    "influxdb3".bold(),
                    "Common Commands:".bold().underline(),
                    "serve".bold(),
                    "query, q".bold(),
                    "write, w".bold(),
                    "update".bold(),
                    "Resource Management:".bold().underline(),
                    "create".bold(),
                    "show".bold(),
                    "delete".bold(),
                    "enable".bold(),
                    "disable".bold(),
                    "System Management:".bold().underline(),
                    "install".bold(),
                    "test".bold(),
                    "Configuration Options:".bold().underline(),
                    "Additional Options:".bold().underline(),
                );
                std::process::exit(0);
            }
            // Some(SubCommand::Enable) => println!(include_str!("help/enable_all.txt")),
            // Some(SubCommand::Create) => println!(include_str!("help/create_all.txt")),
            // Some(SubCommand::Disable) => println!(include_str!("help/disable_all.txt")),
            // Some(SubCommand::Delete) => println!(include_str!("help/delete_all.txt")),
            Some(SubCommand::Serve) => {
                println!(
                    include_str!("help/serve_all.txt"),
                    "Run the InfluxDB 3 Core Server".bold(),
                    "Usage: influxdb3 serve".bold(),
                    "Required:".bold().underline(),
                    "Server Configuration:".bold().underline(),
                    "TLS & Authentication:".bold().underline(),
                    "Storage:".bold().underline(),
                    "AWS S3:".bold(),
                    "Azure Blob:".bold(),
                    "Google Cloud:".bold(),
                    "Object Store Advanced:".bold(),
                    "Processing Engine:".bold().underline(),
                    "Data Lifecycle & Retention:".bold().underline(),
                    "Write-Ahead Log (WAL):".bold().underline(),
                    "Cache Options:".bold().underline(),
                    "HTTP Configuration:".bold().underline(),
                    "Memory Management:".bold().underline(),
                    "DataFusion:".bold().underline(),
                    "Logging and Tracing:".bold().underline(),
                    "Additional Options:".bold().underline(),
                );
                std::process::exit(0);
            }
            // Some(SubCommand::Install) => println!(include_str!("help/install_all.txt")),
            // Some(SubCommand::Show) => println!(include_str!("help/show_all.txt")),
            // Some(SubCommand::Test) => println!(include_str!("help/test_all.txt")),
            // Some(SubCommand::Query) => println!(include_str!("help/query_all.txt")),
            // Some(SubCommand::Write) => println!(include_str!("help/write_all.txt")),
            _ => {}
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
            eprintln!("FATAL Error loading config from: {e}");
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

// Based on ideas from
// https://github.com/servo/servo/blob/f03ddf6c6c6e94e799ab2a3a89660aea4a01da6f/ports/servo/main.rs#L58-L79
#[cfg(unix)]
fn install_crash_handler() {
    unsafe {
        set_signal_handler(libc::SIGSEGV, signal_handler); // handle segfaults
        set_signal_handler(libc::SIGILL, signal_handler); // handle stack overflow and unsupported CPUs
        set_signal_handler(libc::SIGBUS, signal_handler); // handle invalid memory access
    }
}

#[cfg(unix)]
unsafe extern "C" fn signal_handler(_sig: i32) {
    // The commented out code is *not* async signal safe and only a small set of libc functions
    // can be used. See https://man7.org/linux/man-pages/man7/signal-safety.7.html for more
    // information
    //
    // From https://github.com/influxdata/influxdb_pro/issues/971:
    // > The signal_handler implementation calls high-level Rust routines (e.g.
    // > thread name resolution, heap allocation via format!, buffered I/O with
    // > eprintln! and unwinding through backtrace::Backtrace::new()) from within a
    // > POSIX signal context. These operations are not guaranteed to be reentrant
    // > or async-signal-safe, risking corruption of allocator metadata, I/O buffers,
    // > and mutexes if a signal interrupts their internal execution.
    // Until we find a safe way to do this, we will simply abort like we had been
    // doing, but without the extra context.
    //
    // use backtrace::Backtrace;
    // let name = std::thread::current()
    //     .name()
    //     .map(|n| format!(" for thread \"{n}\""))
    //     .unwrap_or_else(|| "".to_owned());
    // eprintln!(
    //     "Signal {}, Stack trace{}\n{:?}",
    //     sig,
    //     name,
    //     Backtrace::new()
    // );

    std::process::abort();
}

// based on https://github.com/adjivas/sig/blob/master/src/lib.rs#L34-L52
#[cfg(unix)]
unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = unsafe { std::mem::zeroed() };

    // Block all signals during the handler. This is the expected behavior, but
    // it's not guaranteed by `signal()`.
    if unsafe { sigfillset(&mut sigset) } != -1 {
        // Done because sigaction has private members.
        // This is safe because sa_restorer and sa_handlers are pointers that
        // might be null (that is, zero).
        let mut action: sigaction = unsafe { std::mem::zeroed() };

        // action.sa_flags = 0;
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;

        unsafe {
            sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}

fn init_logs_and_tracing(
    config: &trogging::cli::LoggingConfig,
) -> Result<TroggingGuard, trogging::Error> {
    let log_layer = trogging::Builder::new()
        .with_default_log_filter(
            "info,iox_query::query_log=warn,influxdb3_query_executor::query_planner=warn",
        )
        .with_logging_config(config)
        .build()?;

    let layers = log_layer;

    // Optionally enable the tokio console exporter layer, if enabled.
    //
    // This spawns a background tokio task to serve the instrumentation data,
    // and hooks the instrumentation into the tracing pipeline.
    #[cfg(feature = "tokio_console")]
    let layers = {
        use console_subscriber::ConsoleLayer;
        let console_layer = ConsoleLayer::builder().with_default_env().spawn();
        layers.and_then(console_layer)
    };

    let subscriber = Registry::default().with(layers);
    trogging::install_global(subscriber)
}

/// Extract user-provided parameters from ArgMatches using clap's ids()
/// Returns a simple map of CLI argument name -> string value for all user-provided arguments
fn extract_user_params(matches: &clap::ArgMatches) -> HashMap<String, String> {
    let mut params = HashMap::new();

    // Check if we're in the serve subcommand
    let serve_matches = if let Some(("serve", sub_matches)) = matches.subcommand() {
        sub_matches
    } else {
        return params; // Only handle serve command for now
    };

    // Get the serve command metadata
    let serve_cmd = commands::serve::Config::command();

    // Iterate through all arguments defined in the serve command
    for arg in serve_cmd.get_arguments() {
        let id = arg.get_id();
        let id_str = id.as_str();

        // Only include arguments that were explicitly provided by the user
        let source = serve_matches.value_source(id_str);
        if source == Some(ValueSource::CommandLine) || source == Some(ValueSource::EnvVariable) {
            // Get display name (prefer long, then short, then id)
            let display_name = arg
                .get_long()
                .map(|s| s.to_string())
                .or_else(|| arg.get_short().map(|c| c.to_string()))
                .unwrap_or_else(|| id.to_string());

            // Skip internal clap arguments
            if display_name == "help" || display_name == "version" || display_name == "help-all" {
                continue;
            }

            // Get the raw values as strings
            if let Some(raw_vals) = serve_matches.get_raw(id_str) {
                let values: Vec<String> = raw_vals
                    .map(|os_str| os_str.to_string_lossy().to_string())
                    .collect();

                if values.len() == 1 {
                    // Single value
                    params.insert(display_name, values[0].clone());
                } else if !values.is_empty() {
                    // Multiple values - join with comma
                    params.insert(display_name, values.join(","));
                }
            } else if serve_matches.get_flag(id_str) {
                // Boolean flag without value
                params.insert(display_name, "true".to_string());
            }
        }
    }

    params
}
