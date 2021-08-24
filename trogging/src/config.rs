#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogFormat {
    Full,
    Pretty,
    Json,
    Logfmt,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            "logfmt" => Ok(Self::Logfmt),
            _ => Err(format!(
                "Invalid log format '{}'. Valid options: full, pretty, json, logfmt",
                s
            )),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Pretty => write!(f, "pretty"),
            Self::Json => write!(f, "json"),
            Self::Logfmt => write!(f, "logfmt"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LogDestination {
    Stdout,
    Stderr,
}

impl std::str::FromStr for LogDestination {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            _ => Err(format!(
                "Invalid log destination '{}'. Valid options: stdout, stderr",
                s
            )),
        }
    }
}

impl std::fmt::Display for LogDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => write!(f, "stdout"),
            Self::Stderr => write!(f, "stderr"),
        }
    }
}
