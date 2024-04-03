use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::line_protocol_generator::WriterId;

/// The specification for the data to be generated
#[derive(Debug, Deserialize, Serialize)]
pub struct DataSpec {
    /// The name of this spec
    pub name: String,
    /// The measurements to be generated for each sample
    pub measurements: Vec<MeasurementSpec>,
}

impl DataSpec {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
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
    /// appended to the measurement name to uniquely identify it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub copies: Option<usize>,
    /// If this measurement has tags with cardinality, this is the number of lines that will
    /// be output per sample (up to the highest cardinality tag). If not specified, all unique
    /// values will be used. Cardinality is split across the number of workers, so the number
    /// of lines per sample could be less than this number.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub lines_per_sample: Option<usize>,
}

impl MeasurementSpec {
    /// Get the max cardinality accross all tags in the spec
    pub fn max_cardinality(&self) -> usize {
        self.tags
            .iter()
            .map(|t| t.cardinality.unwrap_or(1))
            .max()
            .unwrap_or(1)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TagSpec {
    /// the key/name of the tag
    pub key: String,

    /// have this many copies of this tag in the measurement. Random values will be generated
    /// independently (i.e. copies won't share the same random value). Will add the copy number to
    /// the key of the tag to uniquely identify it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub copies: Option<usize>,
    /// if set, appends the copy id of the tag to the value of the tag
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub append_copy_id: Option<bool>,

    /// output this string value for every line this tag is present
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub value: Option<String>,

    /// if set, appends the writer id to the value of the tag
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub append_writer_id: Option<bool>,

    /// will add a number to the value of the tag, with this number of unique values
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cardinality: Option<usize>,
}

impl TagSpec {
    pub fn cardinality_min_max(
        &self,
        writer_id: WriterId,
        writer_count: usize,
    ) -> Option<(usize, usize)> {
        if let Some(cardinality) = self.cardinality {
            let cardinality_increment = usize::div_ceil(cardinality, writer_count);
            let cardinality_id_min = writer_id * cardinality_increment - cardinality_increment + 1;
            let cardinality_id_max = cardinality_id_min + cardinality_increment - 1;

            Some((cardinality_id_min, cardinality_id_max))
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FieldSpec {
    // These options apply to any type of field
    /// the key/name of the field
    pub key: String,
    /// have this many copies of this field in the measurement. Random values will be generated
    /// independently (i.e. copies won't share the same random value). Will add the copy number to
    /// the key of the field to uniquely identify it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub copies: Option<usize>,
    /// A float between 0.0 and 1.0 that determines the probability that this field will be null.
    /// At least one field in a measurement should not have this option set.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub null_probability: Option<f64>,

    #[serde(flatten)]
    pub field: FieldKind,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    /// generates a random bool for the value of this field
    Bool(bool),
    /// output this string value for every line this field is present
    String(String),
    /// generate a random string of this length for every line this field is present
    StringRandom(usize),
    /// output this integer value for every line this field is present
    Integer(i64),
    /// generate a random integer in this range for every line this field is present
    IntegerRange(i64, i64),
    /// output this float value for every line this field is present
    Float(f64),
    /// generate a random float in this range for every line this field is present
    FloatRange(f64, f64),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QuerierSpec {
    /// The name of this spec
    pub name: String,
    /// The queries to run on each interval
    pub queries: Vec<QuerySpec>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QuerySpec {
    /// The literal query string to be executed
    pub query: String,
    /// Define parameters that will be injected into `query`
    #[serde(default)]
    pub params: Vec<ParamSpec>,
}

impl QuerierSpec {
    pub(crate) fn to_json_string_pretty(&self) -> Result<String, anyhow::Error> {
        serde_json::to_string_pretty(self).context("failed to serialize query spec")
    }

    pub(crate) fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let contents = std::fs::read_to_string(path)?;
        serde_json::from_str(&contents).context("unable to serialize as JSON")
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ParamSpec {
    /// The literal parameter name
    ///
    /// If the parameter appears in the query as `$foo`, specify `"foo"` here.
    pub name: String,
    /// The parameter definition
    #[serde(flatten)]
    pub param: ParamKind,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "def")]
pub enum ParamKind {
    Static(Value),
    Cardinality {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        base: Option<String>,
        cardinality: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_spec_splits_cardinality_for_writers() {
        let mut tag_spec = TagSpec {
            key: "".to_string(),
            copies: None,
            append_copy_id: None,
            value: None,
            append_writer_id: None,
            cardinality: Some(100),
        };

        let (min, max) = tag_spec.cardinality_min_max(1, 10).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 10);
        let (min, max) = tag_spec.cardinality_min_max(2, 10).unwrap();
        assert_eq!(min, 11);
        assert_eq!(max, 20);
        let (min, max) = tag_spec.cardinality_min_max(10, 10).unwrap();
        assert_eq!(min, 91);
        assert_eq!(max, 100);

        // if the cardinality is not evenly divisible by the number of writers, the last writer
        // will go over the cardinality set
        tag_spec.cardinality = Some(30);
        let (min, max) = tag_spec.cardinality_min_max(1, 7).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
        let (min, max) = tag_spec.cardinality_min_max(4, 7).unwrap();
        assert_eq!(min, 16);
        assert_eq!(max, 20);
        let (min, max) = tag_spec.cardinality_min_max(7, 7).unwrap();
        assert_eq!(min, 31);
        assert_eq!(max, 35);
    }
}
