use std::{borrow::Cow, convert::TryInto, path::PathBuf, sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use observability_deps::tracing::{debug, info};
use rustyline::{error::ReadlineError, hint::Hinter, Editor};
use snafu::{ResultExt, Snafu};

use super::repl_command::ReplCommand;

use influxdb_iox_client::{connection::Connection, format::QueryOutputFormat};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading command: {}", source))]
    Readline { source: ReadlineError },

    #[snafu(display("Error loading remote state: {}", source))]
    LoadingRemoteState {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error formatting results: {}", source))]
    FormattingResults {
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display("Error setting format to '{}': {}", requested_format, source))]
    SettingFormat {
        requested_format: String,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display("Error parsing command: {}", message))]
    ParsingCommand { message: String },

    #[snafu(display("Error running remote query: {}", source))]
    RunningRemoteQuery {
        source: influxdb_iox_client::flight::Error,
    },

    #[snafu(display("Cannot create REPL: {}", source))]
    ReplCreation { source: ReadlineError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

enum QueryEngine {
    /// Run queries against the namespace on the remote server
    Remote(String),
}

struct RustylineHelper {
    hinter: rustyline::hint::HistoryHinter,
    highlighter: rustyline::highlight::MatchingBracketHighlighter,
}

impl Default for RustylineHelper {
    fn default() -> Self {
        Self {
            hinter: rustyline::hint::HistoryHinter {},
            highlighter: rustyline::highlight::MatchingBracketHighlighter::default(),
        }
    }
}

impl rustyline::Helper for RustylineHelper {}

impl rustyline::validate::Validator for RustylineHelper {
    fn validate(
        &self,
        ctx: &mut rustyline::validate::ValidationContext<'_>,
    ) -> rustyline::Result<rustyline::validate::ValidationResult> {
        let input = ctx.input();

        if input.trim_end().ends_with(';') {
            match ReplCommand::try_from(input) {
                Ok(_) => Ok(rustyline::validate::ValidationResult::Valid(None)),
                Err(err) => Ok(rustyline::validate::ValidationResult::Invalid(Some(err))),
            }
        } else {
            Ok(rustyline::validate::ValidationResult::Incomplete)
        }
    }
}

impl rustyline::hint::Hinter for RustylineHelper {
    type Hint = String;
    fn hint(&self, line: &str, pos: usize, ctx: &rustyline::Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl rustyline::highlight::Highlighter for RustylineHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        self.highlighter.highlight_prompt(prompt, default)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        // TODO Detect when windows supports ANSI escapes
        #[cfg(windows)]
        {
            Cow::Borrowed(hint)
        }
        #[cfg(not(windows))]
        {
            use nu_ansi_term::Style;
            Cow::Owned(Style::new().dimmed().paint(hint).to_string())
        }
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        completion: rustyline::CompletionType,
    ) -> Cow<'c, str> {
        self.highlighter.highlight_candidate(candidate, completion)
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }
}

impl rustyline::completion::Completer for RustylineHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        // If there is a hint, use that as the auto-complete when user hits `tab`
        if let Some(hint) = self.hinter.hint(line, pos, ctx) {
            let start_pos = pos;
            Ok((start_pos, vec![hint]))
        } else {
            Ok((0, Vec::with_capacity(0)))
        }
    }
}

/// Captures the state of the repl, gathers commands and executes them
/// one by one
pub struct Repl {
    /// Rustyline editor for interacting with user on command line
    rl: Editor<RustylineHelper>,

    /// Current prompt
    prompt: String,

    /// Client for interacting with IOx namespace API
    namespace_client: influxdb_iox_client::namespace::Client,

    /// Client for running sql
    flight_client: influxdb_iox_client::flight::Client,

    /// namespace name against which SQL commands are run
    query_engine: Option<QueryEngine>,

    /// Formatter to use to format query results
    output_format: QueryOutputFormat,
}

impl Repl {
    fn print_help(&self) {
        print!("{}", ReplCommand::help())
    }

    /// Create a new Repl instance, connected to the specified URL
    pub fn new(connection: Connection) -> Result<Self> {
        let namespace_client = influxdb_iox_client::namespace::Client::new(connection.clone());
        let flight_client = influxdb_iox_client::flight::Client::new(connection);

        let mut rl = Editor::new().context(ReplCreationSnafu)?;
        rl.set_helper(Some(RustylineHelper::default()));
        let history_file = history_file();

        if let Err(e) = rl.load_history(&history_file) {
            debug!(%e, "error loading history file");
        }

        let prompt = "> ".to_string();

        let output_format = QueryOutputFormat::Pretty;

        Ok(Self {
            rl,
            prompt,
            namespace_client,
            flight_client,
            query_engine: None,
            output_format,
        })
    }

    /// Read Evaluate Print Loop (interactive command line) for SQL
    ///
    /// Inspired / based on repl.rs from DataFusion
    pub async fn run(&mut self) -> Result<()> {
        println!("Ready for commands. (Hint: try 'help;')");
        loop {
            match self.next_command()? {
                ReplCommand::Help => {
                    self.print_help();
                }
                ReplCommand::ShowNamespaces => {
                    self.list_namespaces()
                        .await
                        .map_err(|e| println!("{e}"))
                        .ok();
                }
                ReplCommand::UseNamespace { db_name } => {
                    self.use_namespace(db_name);
                }
                ReplCommand::SqlCommand { sql } => {
                    self.run_sql(sql).await.map_err(|e| println!("{e}")).ok();
                }
                ReplCommand::Exit => {
                    info!("exiting at user request");
                    return Ok(());
                }
                ReplCommand::SetFormat { format } => {
                    self.set_output_format(format)?;
                }
            }
        }
    }

    /// Parss the next command;
    fn next_command(&mut self) -> Result<ReplCommand> {
        match self.rl.readline(&self.prompt) {
            Ok(ref line) if is_exit_command(line) => Ok(ReplCommand::Exit),
            Ok(ref line) => {
                let request = line.trim_end();
                self.rl.add_history_entry(request.to_owned());

                request
                    .try_into()
                    .map_err(|message| Error::ParsingCommand { message })
            }
            Err(ReadlineError::Eof) => {
                debug!("Received Ctrl-D");
                Ok(ReplCommand::Exit)
            }
            Err(ReadlineError::Interrupted) => {
                debug!("Received Ctrl-C");
                Ok(ReplCommand::Exit)
            }
            // Some sort of real underlying error
            Err(e) => Err(Error::Readline { source: e }),
        }
    }

    // print all namespaces to the output
    async fn list_namespaces(&mut self) -> Result<()> {
        let namespaces = self
            .namespace_client
            .get_namespaces()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(LoadingRemoteStateSnafu)?;

        let namespace_id: Int64Array = namespaces.iter().map(|ns| Some(ns.id)).collect();
        let name: StringArray = namespaces.iter().map(|ns| Some(&ns.name)).collect();

        let record_batch = RecordBatch::try_from_iter(vec![
            ("namespace_id", Arc::new(namespace_id) as ArrayRef),
            ("name", Arc::new(name) as ArrayRef),
        ])
        .expect("creating record batch successfully");

        self.print_results(&[record_batch])
    }

    // Run a command against the currently selected remote namespace
    async fn run_sql(&mut self, sql: String) -> Result<()> {
        let start = Instant::now();

        let batches: Vec<_> = match &self.query_engine {
            None => {
                println!("Error: no namespace selected.");
                println!("Hint: Run USE NAMESPACE <dbname> to select namespace");
                return Ok(());
            }
            Some(QueryEngine::Remote(db_name)) => {
                info!(%db_name, %sql, "Running sql on remote namespace");

                self.flight_client
                    .sql(db_name.to_string(), sql)
                    .await
                    .context(RunningRemoteQuerySnafu)?
                    .try_collect()
                    .await
                    .context(RunningRemoteQuerySnafu)?
            }
        };

        let end = Instant::now();
        self.print_results(&batches)?;

        println!(
            "Returned {} in {:?}",
            Self::row_summary(&batches),
            end - start
        );
        Ok(())
    }

    fn row_summary<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> String {
        let total_rows: usize = batches.into_iter().map(|b| b.num_rows()).sum();

        if total_rows > 1 {
            format!("{total_rows} rows")
        } else if total_rows == 0 {
            "no rows".to_string()
        } else {
            "1 row".to_string()
        }
    }

    fn use_namespace(&mut self, db_name: String) {
        info!(%db_name, "setting current namespace");
        println!("You are now in remote mode, querying namespace {db_name}");
        self.set_query_engine(QueryEngine::Remote(db_name));
    }

    fn set_query_engine(&mut self, query_engine: QueryEngine) {
        self.prompt = match &query_engine {
            QueryEngine::Remote(db_name) => {
                format!("{db_name}> ")
            }
        };
        self.query_engine = Some(query_engine)
    }

    /// Sets the output format to the specified format
    pub fn set_output_format<S: AsRef<str>>(&mut self, requested_format: S) -> Result<()> {
        let requested_format = requested_format.as_ref();

        self.output_format = requested_format
            .parse()
            .context(SettingFormatSnafu { requested_format })?;
        println!("Set output format format to {}", self.output_format);
        Ok(())
    }

    /// Prints to the specified output format
    fn print_results(&self, batches: &[RecordBatch]) -> Result<()> {
        let formatted_results = self
            .output_format
            .format(batches)
            .context(FormattingResultsSnafu)?;
        println!("{formatted_results}");
        Ok(())
    }
}

impl Drop for Repl {
    fn drop(&mut self) {
        let history_file = history_file();

        if let Err(e) = self.rl.save_history(&history_file) {
            debug!(%e, "error saving history file");
        }
    }
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

/// Return the location of the history file (defaults to $HOME/".iox_sql_history")
fn history_file() -> PathBuf {
    let mut buf = match std::env::var("HOME") {
        Ok(home) => PathBuf::from(home),
        Err(_) => PathBuf::new(),
    };
    buf.push(".iox_sql_history");
    buf
}
