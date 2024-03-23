use anyhow::Context;
use serde::{Deserialize, Serialize};

/// The specification for the data to be generated
#[derive(Debug, Deserialize, Serialize)]
pub struct DataSpec {
    /// The name of this spec
    pub name: String,
    /// The measurements to be generated for each sample
    pub measurements: Vec<MeasurementSpec>,
}

impl DataSpec {
    pub fn from_path(path: &str) -> Result<Self, anyhow::Error> {
        let contents = std::fs::read_to_string(path)?;
        let res = serde_json::from_str(&contents)?;

        Ok(res)
    }

    pub fn to_json_string_pretty(&self) -> Result<String, anyhow::Error> {
        let res = serde_json::to_string_pretty(&self).context("failed to encode json to string")?;
        Ok(res)
    }
}

/// Specification for a measurement to be generated
#[derive(Debug, Deserialize, Serialize)]
pub struct MeasurementSpec {
    /// The name of the measurement
    pub name: String,
    /// The tags to be generated for each line
    pub tags: Vec<TagSpec>,
    /// The fields to be generated for each line
    pub fields: Vec<FieldSpec>,
    /// Create this many copies of this measurement in each sample. The copy number will be
    /// appended to the measurement name to uniquely identify it./
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copies: Option<usize>,
    /// If this measurement has tags with cardinality, this is the number of lines that will
    /// be output per sample go through the cardinality. If not specified, all unique values
    /// will be used. This number must be <= cardinality of the highest cardinality tag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lines_per_sample: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagSpec {
    /// the key/name of the tag
    pub key: String,

    /// have this many copies of this tag in the measurement. Random values will be generated
    /// independently (i.e. copies won't share the same random value). Will add the copy number to
    /// the key of the tag to uniquely identify it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copies: Option<usize>,
    /// if set, appends the copy id of the tag to the value of the tag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append_copy_id: Option<bool>,

    /// output this string value for every line this tag is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// if set, appends the writer id to the value of the tag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append_writer_id: Option<bool>,

    /// will add a number to the value of the tag, with this number of unique values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FieldSpec {
    // These options apply to any type of field
    /// the key/name of the field
    pub key: String,
    /// have this many copies of this field in the measurement. Random values will be generated
    /// independently (i.e. copies won't share the same random value). Will add the copy number to
    /// the key of the field to uniquely identify it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copies: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_probability: Option<f64>,

    // These options apply to specific types of fields and are mutually exclusive
    /// generates a random bool for the value of this field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bool: Option<bool>,

    /// output this string value for every line this field is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string: Option<String>,
    // generate a random string of this length to append to earlier string for every line this field is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_random: Option<usize>,

    /// output this integer value for every line this field fis present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integer: Option<i64>,
    // generate a random integer in this range for every line this field is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integer_range: Option<(i64, i64)>,

    /// output this float value for every line this field is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub float: Option<f64>,
    /// generate a random float in this range for every line this field is present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub float_range: Option<(f64, f64)>,
}
