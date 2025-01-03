#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Uint64,
    Float64,
    Utf8,
    Bool,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0} is not a valid data type, values are int64, uint64, float64, utf8, and bool")]
pub struct ParseDataTypeError(String);

impl FromStr for DataType {
    type Err = ParseDataTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int64" => Ok(Self::Int64),
            "uint64" => Ok(Self::Uint64),
            "float64" => Ok(Self::Float64),
            "utf8" => Ok(Self::Utf8),
            "bool" => Ok(Self::Bool),
            _ => Err(ParseDataTypeError(s.into())),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64 => write!(f, "int64"),
            Self::Uint64 => write!(f, "uint64"),
            Self::Float64 => write!(f, "float64"),
            Self::Utf8 => write!(f, "utf8"),
            Self::Bool => write!(f, "bool"),
        }
    }
}

impl From<DataType> for String {
    fn from(data: DataType) -> Self {
        data.to_string()
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find(':')
        .ok_or_else(|| format!("invalid FIELD:VALUE. No `:` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
