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

use clap::Parser;
use dotenvy::dotenv;
use influxdb3_clap_blocks::tokio::{TokioDatafusionConfig, TokioIoConfig};
use influxdb3_process::VERSION_STRING;
use observability_deps::tracing::warn;
use owo_colors::OwoColorize;

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
    pub mod install;
    pub mod query;
    pub mod serve;
    pub mod show;
    pub mod test;
    pub mod write;
}

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

    /// Perform a set of writes to a running InfluxDB 3 Core server
    Write(commands::write::Config),
}

fn main() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    install_crash_handler(); // attempt to render a useful stacktrace to stderr

    // load all environment variables from .env before doing anything
    load_dotenv();

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
    let config = Config::parse();

    let tokio_runtime = config.runtime_config.builder()?.build()?;

    tokio_runtime.block_on(async move {
        fn handle_init_logs(r: Result<TroggingGuard, trogging::Error>) -> TroggingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {e}");
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }

        match config.command {
            None => println!("command required, -h/--help/--help-all for help"),
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
            Some(Command::Serve(config)) => {
                let _tracing_guard =
                    handle_init_logs(init_logs_and_tracing(&config.logging_config));
                if let Err(e) = commands::serve::command(config).await {
                    eprintln!("Serve command failed: {e}");
                    std::process::exit(ReturnCode::Failure as _)
                }
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
                    "Aditional Options:".bold().underline(),
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
                    "Usage: influxdb3 serve".bold(),
                    "Required:".bold().underline(),
                    "Common Options:".bold().underline(),
                    "Storage Options:".bold().underline(),
                    "AWS S3 Storage:".bold().underline(),
                    "Google Cloud Storage:".bold().underline(),
                    "Azure Blob Storage:".bold().underline(),
                    "Processing Engine Options:".bold().underline(),
                    "Object Store Connection:".bold().underline(),
                    "Network Options:".bold().underline(),
                    "Memory Management:".bold().underline(),
                    "WAL Configuration:".bold().underline(),
                    "Cache Options:".bold().underline(),
                    "Datafusion Configuration:".bold().underline(),
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
unsafe extern "C" fn signal_handler(sig: i32) {
    use backtrace::Backtrace;
    use std::process::abort;
    let name = std::thread::current()
        .name()
        .map(|n| format!(" for thread \"{n}\""))
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
        .with_default_log_filter("info")
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
