use std::convert::TryInto;

use observability_deps::tracing::{debug, warn};

/// Represents the parsed command from the user (which may be over many lines)
#[derive(Debug, PartialEq)]
pub enum ReplCommand {
    Help,
    ShowDatabases,
    Observer,
    SetFormat { format: String },
    UseDatabase { db_name: String },
    SqlCommand { sql: String },
    Exit,
}

impl TryInto<ReplCommand> for String {
    type Error = Self;

    #[allow(clippy::if_same_then_else)]
    fn try_into(self) -> Result<ReplCommand, Self::Error> {
        debug!(%self, "tokenizing to ReplCommand");

        if self.trim().is_empty() {
            return Err("No command specified".to_string());
        }

        // tokenized commands, normalized whitespace but original case
        let raw_commands = self
            .trim()
            // chop off trailing semicolon
            .strip_suffix(";")
            .unwrap_or(&self)
            // tokenize on whitespace
            .split(' ')
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .collect::<Vec<_>>();

        // normalized commands (all lower case)
        let commands = raw_commands
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect::<Vec<_>>();

        debug!(?raw_commands, ?commands, "processing tokens");

        // Get something we can more easily pattern match on
        let commands = commands.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        match commands.as_slice() {
            ["help"] => Ok(ReplCommand::Help),
            ["help", ..] => {
                let extra_content = commands[1..].join(" ");
                warn!(%extra_content, "ignoring tokens after 'help'");
                Ok(ReplCommand::Help)
            }
            ["observer"] => Ok(ReplCommand::Observer),
            ["exit"] => Ok(ReplCommand::Exit),
            ["quit"] => Ok(ReplCommand::Exit),
            ["use", "database"] => {
                Err("name not specified. Usage: USE DATABASE <name>".to_string())
            } // USE DATABASE
            ["use", "database", _name] => {
                // USE DATABASE <name>
                Ok(ReplCommand::UseDatabase {
                    db_name: raw_commands[2].to_string(),
                })
            }
            ["use", _command] => {
                // USE <name>
                Ok(ReplCommand::UseDatabase {
                    db_name: raw_commands[1].to_string(),
                })
            }
            ["show", "databases"] => Ok(ReplCommand::ShowDatabases),
            ["set", "format", _format] => Ok(ReplCommand::SetFormat {
                format: raw_commands[2].to_string(),
            }),
            _ => {
                // By default, treat the entire string as SQL
                Ok(ReplCommand::SqlCommand { sql: self })
            }
        }
    }
}

impl ReplCommand {
    /// Information for each command
    pub fn help() -> &'static str {
        r#"
Available commands (not case sensitive):
HELP (this one)

SHOW DATABASES: List databases available on the server

USE [DATABASE] <name>: Set the current remote database to name

SET FORMAT <format>: Set the output format to Pretty, csv or json

OBSERVER: Locally query unified queryable views of remote system tables

[EXIT | QUIT]: Quit this session and exit the program

# Examples: use remote database foo
SHOW DATABASES;
USE DATABASE foo;

# Basic IOx SQL Primer

;; Explore Schema:
SHOW TABLES; ;; Show available tables
SHOW COLUMNS FROM my_table; ;; Show columns in the table

;; Show storage usage across partitions and tables
SELECT
   partition_key, table_name, storage,
   count(*) as chunk_count,
   sum(memory_bytes)/(1024*1024) as size_mb
FROM
  system.chunks
GROUP BY
   partition_key, table_name, storage
ORDER BY
  size_mb DESC
LIMIT 20
;

"#
    }
}

#[cfg(test)]
impl TryInto<ReplCommand> for &str {
    type Error = String;

    fn try_into(self) -> Result<ReplCommand, Self::Error> {
        self.to_string().try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::unnecessary_wraps)]
    fn sql_cmd(sql: &str) -> Result<ReplCommand, String> {
        Ok(ReplCommand::SqlCommand {
            sql: sql.to_string(),
        })
    }

    #[test]
    fn empty() {
        let expected: Result<ReplCommand, String> = Err("No command specified".to_string());

        assert_eq!("".try_into(), expected);
        assert_eq!("  ".try_into(), expected);
        assert_eq!(" \t".try_into(), expected);
    }

    #[test]
    fn help() {
        let expected = Ok(ReplCommand::Help);
        assert_eq!("help;".try_into(), expected);
        assert_eq!("help".try_into(), expected);
        assert_eq!("  help".try_into(), expected);
        assert_eq!("  help  ".try_into(), expected);
        assert_eq!("  HELP  ".try_into(), expected);
        assert_eq!("  Help;  ".try_into(), expected);
        assert_eq!("  help  ; ".try_into(), expected);
        assert_eq!("  help me;  ".try_into(), expected);
    }

    #[test]
    fn observer() {
        let expected = Ok(ReplCommand::Observer);
        assert_eq!("observer;".try_into(), expected);
        assert_eq!("observer".try_into(), expected);
        assert_eq!("  observer".try_into(), expected);
        assert_eq!("  observer  ".try_into(), expected);
        assert_eq!("  OBSERVER  ".try_into(), expected);
        assert_eq!("  Observer;  ".try_into(), expected);
        assert_eq!("  observer  ; ".try_into(), expected);

        let expected = sql_cmd("  observer me;  ");
        assert_eq!("  observer me;  ".try_into(), expected);
    }

    #[test]
    fn show_databases() {
        let expected = Ok(ReplCommand::ShowDatabases);
        assert_eq!("show databases".try_into(), expected);
        assert_eq!("show  Databases".try_into(), expected);
        assert_eq!("show  databases;".try_into(), expected);
        assert_eq!("SHOW DATABASES".try_into(), expected);

        assert_eq!("SHOW DATABASES DD".try_into(), sql_cmd("SHOW DATABASES DD"));
    }

    #[test]
    fn use_database() {
        let expected = Ok(ReplCommand::UseDatabase {
            db_name: "Foo".to_string(),
        });
        assert_eq!("use Foo".try_into(), expected);
        assert_eq!("use Database Foo;".try_into(), expected);
        assert_eq!("use Database Foo ;".try_into(), expected);
        assert_eq!(" use Database Foo;   ".try_into(), expected);
        assert_eq!("   use Database Foo;   ".try_into(), expected);

        // ensure that database name is case sensitive
        let expected = Ok(ReplCommand::UseDatabase {
            db_name: "FOO".to_string(),
        });
        assert_eq!("use FOO".try_into(), expected);
        assert_eq!("use DATABASE FOO;".try_into(), expected);
        assert_eq!("USE DATABASE FOO;".try_into(), expected);

        let expected: Result<ReplCommand, String> =
            Err("name not specified. Usage: USE DATABASE <name>".to_string());
        assert_eq!("use Database;".try_into(), expected);
        assert_eq!("use DATABASE".try_into(), expected);
        assert_eq!("use database".try_into(), expected);

        let expected = sql_cmd("use database foo bar");
        assert_eq!("use database foo bar".try_into(), expected);

        let expected = sql_cmd("use database foo BAR");
        assert_eq!("use database foo BAR".try_into(), expected);
    }

    #[test]
    fn set_format() {
        let expected = Ok(ReplCommand::SetFormat {
            format: "csv".to_string(),
        });
        assert_eq!(" set format csv".try_into(), expected);
        assert_eq!("SET format   csv;".try_into(), expected);
        assert_eq!("set  format csv".try_into(), expected);
        assert_eq!("set format csv;".try_into(), expected);

        let expected = Ok(ReplCommand::SetFormat {
            format: "Hmm".to_string(),
        });
        assert_eq!("set format Hmm".try_into(), expected);
    }

    #[test]
    fn sql_command() {
        let expected = sql_cmd("SELECT * from foo");
        assert_eq!("SELECT * from foo".try_into(), expected);
        // ensure that we aren't messing with capitalization
        assert_ne!("select * from foo".try_into(), expected);

        let expected = sql_cmd("select * from foo");
        assert_eq!("select * from foo".try_into(), expected);

        // default to sql command
        let expected = sql_cmd("blah");
        assert_eq!("blah".try_into(), expected);
    }

    #[test]
    fn exit() {
        let expected = Ok(ReplCommand::Exit);
        assert_eq!("exit".try_into(), expected);
        assert_eq!("exit;".try_into(), expected);
        assert_eq!("exit ;".try_into(), expected);
        assert_eq!("EXIT".try_into(), expected);

        assert_eq!("quit".try_into(), expected);
        assert_eq!("quit;".try_into(), expected);
        assert_eq!("quit ;".try_into(), expected);
        assert_eq!("QUIT".try_into(), expected);

        let expected = sql_cmd("quit dragging");
        assert_eq!("quit dragging".try_into(), expected);
    }
}
