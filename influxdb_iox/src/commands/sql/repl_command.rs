use observability_deps::tracing::{debug, warn};

/// Represents the parsed command from the user (which may be over many lines)
#[derive(Debug, PartialEq, Eq)]
pub enum ReplCommand {
    Help,
    ShowNamespaces,
    SetFormat { format: String },
    UseNamespace { db_name: String },
    SqlCommand { sql: String },
    Exit,
}

impl TryFrom<String> for ReplCommand {
    type Error = String;

    fn try_from(input: String) -> Result<Self, Self::Error> {
        Self::try_from(&input[..])
    }
}

impl TryFrom<&str> for ReplCommand {
    type Error = String;

    #[allow(clippy::if_same_then_else)]
    fn try_from(input: &str) -> Result<Self, Self::Error> {
        debug!(%input, "tokenizing to ReplCommand");

        if input.trim().is_empty() {
            return Err("No command specified".to_string());
        }

        // tokenized commands, normalized whitespace but original case
        let raw_commands = input
            .trim()
            // chop off trailing semicolon
            .strip_suffix(';')
            .unwrap_or(input)
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
            ["help"] => Ok(Self::Help),
            ["help", ..] => {
                let extra_content = commands[1..].join(" ");
                warn!(%extra_content, "ignoring tokens after 'help'");
                Ok(Self::Help)
            }
            ["exit"] => Ok(Self::Exit),
            ["quit"] => Ok(Self::Exit),
            ["use", "namespace"] => {
                Err("name not specified. Usage: USE NAMESPACE <name>".to_string())
            } // USE NAMESPACE
            ["use", "namespace", _name] => {
                // USE namespace <name>
                Ok(Self::UseNamespace {
                    db_name: raw_commands[2].to_string(),
                })
            }
            ["use", _command] => {
                // USE <name>
                Ok(Self::UseNamespace {
                    db_name: raw_commands[1].to_string(),
                })
            }
            ["show", "namespaces"] => Ok(Self::ShowNamespaces),
            ["set", "format", _format] => Ok(Self::SetFormat {
                format: raw_commands[2].to_string(),
            }),
            _ => {
                // By default, treat the entire string as SQL
                Ok(Self::SqlCommand { sql: input.into() })
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

SHOW NAMESPACES: List namespaces available on the server

USE NAMESPACE <name>: Set the current remote namespace to name

SET FORMAT <format>: Set the output format to Pretty, csv or json

[EXIT | QUIT]: Quit this session and exit the program

# Examples: use remote namespace foo
SHOW NAMESPACES;
USE foo;

# Basic IOx SQL Primer

;; Explore Schema:
SHOW TABLES; ;; Show available tables
SHOW COLUMNS FROM my_table; ;; Show columns in the table

"#
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
    fn show_namespaces() {
        let expected = Ok(ReplCommand::ShowNamespaces);
        assert_eq!("show namespaces".try_into(), expected);
        assert_eq!("show  Namespaces".try_into(), expected);
        assert_eq!("show  namespaces;".try_into(), expected);
        assert_eq!("SHOW NAMESPACES".try_into(), expected);

        assert_eq!(
            "SHOW NAMESPACES DD".try_into(),
            sql_cmd("SHOW NAMESPACES DD")
        );
    }

    #[test]
    fn use_namespace() {
        let expected = Ok(ReplCommand::UseNamespace {
            db_name: "Foo".to_string(),
        });
        assert_eq!("use Foo".try_into(), expected);
        assert_eq!("use Namespace Foo;".try_into(), expected);
        assert_eq!("use Namespace Foo ;".try_into(), expected);
        assert_eq!(" use Namespace Foo;   ".try_into(), expected);
        assert_eq!("   use Namespace Foo;   ".try_into(), expected);

        // ensure that namespace name is case sensitive
        let expected = Ok(ReplCommand::UseNamespace {
            db_name: "FOO".to_string(),
        });
        assert_eq!("use FOO".try_into(), expected);
        assert_eq!("use NAMESPACE FOO;".try_into(), expected);
        assert_eq!("USE NAMESPACE FOO;".try_into(), expected);

        let expected: Result<ReplCommand, String> =
            Err("name not specified. Usage: USE NAMESPACE <name>".to_string());
        assert_eq!("use Namespace;".try_into(), expected);
        assert_eq!("use NAMESPACE".try_into(), expected);
        assert_eq!("use namespace".try_into(), expected);

        let expected = sql_cmd("use namespace foo bar");
        assert_eq!("use namespace foo bar".try_into(), expected);

        let expected = sql_cmd("use namespace foo BAR");
        assert_eq!("use namespace foo BAR".try_into(), expected);
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
