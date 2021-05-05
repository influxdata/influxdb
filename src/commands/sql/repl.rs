use std::{convert::TryInto, path::PathBuf, sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use observability_deps::tracing::{debug, info};
use rustyline::{error::ReadlineError, Editor};
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

    #[snafu(display("Error running observer query: {}", source))]
    RunningObserverQuery { source: super::observer::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// (Potentially) cached remote state of the server
struct RemoteState {
    db_names: Vec<String>,
}

impl RemoteState {
    async fn try_new(
        management_client: &mut influxdb_iox_client::management::Client,
    ) -> Result<Self> {
        let db_names = management_client
            .list_databases()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(LoadingRemoteState)?;

        Ok(Self { db_names })
    }
}

enum QueryEngine {
    /// Run queries against the named database on the remote server
    Remote(String),

    /// Run queries against a local `Observer` instance
    Observer(super::observer::Observer),
}

/// Captures the state of the repl, gathers commands and executes them
/// one by one
pub struct Repl {
    /// Rustyline editor for interacting with user on command line
    rl: Editor<()>,

    /// Current prompt
    prompt: String,

    /// Connection to the server
    connection: Connection,

    /// Client for interacting with IOx management API
    management_client: influxdb_iox_client::management::Client,

    /// Client for running sql
    flight_client: influxdb_iox_client::flight::Client,

    /// database name against which SQL commands are run
    query_engine: Option<QueryEngine>,

    /// Formatter to use to format query results
    output_format: QueryOutputFormat,
}

impl Repl {
    fn print_help(&self) {
        print!("{}", ReplCommand::help())
    }

    /// Create a new Repl instance, connected to the specified URL
    pub fn new(connection: Connection) -> Self {
        let management_client = influxdb_iox_client::management::Client::new(connection.clone());
        let flight_client = influxdb_iox_client::flight::Client::new(connection.clone());

        let mut rl = Editor::<()>::new();
        let history_file = history_file();

        if let Err(e) = rl.load_history(&history_file) {
            debug!(%e, "error loading history file");
        }

        let prompt = "> ".to_string();

        let output_format = QueryOutputFormat::Pretty;

        Self {
            rl,
            prompt,
            connection,
            management_client,
            flight_client,
            query_engine: None,
            output_format,
        }
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
                ReplCommand::Observer {} => {
                    self.use_observer()
                        .await
                        .map_err(|e| println!("{}", e))
                        .ok();
                }
                ReplCommand::ShowDatabases => {
                    self.list_databases()
                        .await
                        .map_err(|e| println!("{}", e))
                        .ok();
                }
                ReplCommand::UseDatabase { db_name } => {
                    self.use_database(db_name);
                }
                ReplCommand::SqlCommand { sql } => {
                    self.run_sql(sql).await.map_err(|e| println!("{}", e)).ok();
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
        let mut request = "".to_owned();
        loop {
            match self.rl.readline(&self.prompt) {
                Ok(ref line) if is_exit_command(line) && request.is_empty() => {
                    return Ok(ReplCommand::Exit);
                }
                Ok(ref line) if line.trim_end().ends_with(';') => {
                    request.push_str(line.trim_end());
                    self.rl.add_history_entry(request.clone());

                    return request
                        .try_into()
                        .map_err(|message| Error::ParsingCommand { message });
                }
                Ok(ref line) => {
                    request.push_str(line);
                    request.push(' ');
                }
                Err(ReadlineError::Eof) => {
                    debug!("Received Ctrl-D");
                    return Ok(ReplCommand::Exit);
                }
                Err(ReadlineError::Interrupted) => {
                    debug!("Received Ctrl-C");
                    return Ok(ReplCommand::Exit);
                }
                // Some sort of real underlying error
                Err(e) => {
                    return Err(Error::Readline { source: e });
                }
            }
        }
    }

    // print all databases to the output
    async fn list_databases(&mut self) -> Result<()> {
        let state = self.remote_state().await?;
        let db_names = StringArray::from_iter_values(state.db_names.iter().map(|s| s.as_str()));

        let record_batch =
            RecordBatch::try_from_iter(vec![("db_name", Arc::new(db_names) as ArrayRef)])
                .expect("creating record batch successfully");

        self.print_results(&[record_batch])
    }

    // Run a command against the currently selected remote database
    async fn run_sql(&mut self, sql: String) -> Result<()> {
        let start = Instant::now();

        let batches = match &mut self.query_engine {
            None => {
                println!("Error: no database selected.");
                println!("Hint: Run USE DATABASE <dbname> to select database");
                return Ok(());
            }
            Some(QueryEngine::Remote(db_name)) => {
                info!(%db_name, %sql, "Running sql on remote database");

                scrape_query(&mut self.flight_client, &db_name, &sql).await?
            }
            Some(QueryEngine::Observer(observer)) => {
                info!("Running sql on local observer");
                observer
                    .run_query(&sql)
                    .await
                    .context(RunningObserverQuery)?
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
            format!("{} rows", total_rows)
        } else if total_rows == 0 {
            "no rows".to_string()
        } else {
            "1 row".to_string()
        }
    }

    fn use_database(&mut self, db_name: String) {
        info!(%db_name, "setting current database");
        println!("You are now in remote mode, querying database {}", db_name);
        self.set_query_engine(QueryEngine::Remote(db_name));
    }

    async fn use_observer(&mut self) -> Result<()> {
        println!("Preparing local views of remote system tables");
        let observer = super::observer::Observer::try_new(self.connection.clone())
            .await
            .context(RunningObserverQuery)?;
        println!("{}", observer.help());
        self.set_query_engine(QueryEngine::Observer(observer));
        Ok(())
    }

    fn set_query_engine(&mut self, query_engine: QueryEngine) {
        self.prompt = match &query_engine {
            QueryEngine::Remote(db_name) => {
                format!("{}> ", db_name)
            }
            QueryEngine::Observer(_) => "OBSERVER> ".to_string(),
        };
        self.query_engine = Some(query_engine)
    }

    /// Sets the output format to the specified format
    pub fn set_output_format<S: AsRef<str>>(&mut self, requested_format: S) -> Result<()> {
        let requested_format = requested_format.as_ref();

        self.output_format = requested_format
            .parse()
            .context(SettingFormat { requested_format })?;
        println!("Set output format format to {}", self.output_format);
        Ok(())
    }

    // TODO make a setting for changing if we cache remote state or not
    async fn remote_state(&mut self) -> Result<RemoteState> {
        let state = RemoteState::try_new(&mut self.management_client).await?;
        Ok(state)
    }

    /// Prints to the specified output format
    fn print_results(&self, batches: &[RecordBatch]) -> Result<()> {
        let formatted_results = self
            .output_format
            .format(batches)
            .context(FormattingResults)?;
        println!("{}", formatted_results);
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

/// Runs the specified `query` and returns the record batches of the result
async fn scrape_query(
    client: &mut influxdb_iox_client::flight::Client,
    db_name: &str,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let mut query_results = client
        .perform_query(db_name, query)
        .await
        .context(RunningRemoteQuery)?;

    let mut batches = vec![];

    while let Some(data) = query_results.next().await.context(RunningRemoteQuery)? {
        batches.push(data);
    }

    Ok(batches)
}
