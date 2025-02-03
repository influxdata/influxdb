use std::slice::IterMut;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::specification::{ParamKind, ParamSpec, QuerierSpec, QuerySpec};

pub type QuerierId = usize;

/// Create a set of queriers to perform queries in parallel
pub fn create_queriers(
    spec: &QuerierSpec,
    format: Format,
    querier_count: usize,
) -> Result<Vec<Querier>, anyhow::Error> {
    let mut generators = vec![];
    for querier_id in 1..querier_count + 1 {
        let mut queries = vec![];
        for q in &spec.queries {
            queries.extend(create_queries(q, querier_id, querier_count))
        }
        generators.push(Querier {
            querier_id,
            format,
            queries,
        })
    }
    Ok(generators)
}

/// Generate a set of queries off of a query spec
///
/// This produces a Vec so that certain [`QuerySpec`]s can result in multiple
/// queries per interval.
fn create_queries(spec: &QuerySpec, querier_id: QuerierId, querier_count: usize) -> Vec<Query> {
    let query = spec.query.clone();
    let mut params = vec![];
    for p in &spec.params {
        params.push(Param::initialize(p, querier_id, querier_count));
    }
    vec![Query { query, params }]
}

#[derive(Debug)]
pub struct Querier {
    pub querier_id: QuerierId,
    pub format: Format,
    pub queries: Vec<Query>,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, ValueEnum)]
pub enum Format {
    #[default]
    Json,
    Csv,
}

impl From<Format> for influxdb3_types::http::QueryFormat {
    fn from(format: Format) -> Self {
        match format {
            Format::Json => Self::Json,
            Format::Csv => Self::Csv,
        }
    }
}

#[derive(Debug)]
pub struct Query {
    query: String,
    params: Vec<Param>,
}

impl Query {
    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn params_mut(&mut self) -> IterMut<'_, Param> {
        self.params.iter_mut()
    }
}

#[derive(Debug)]
pub struct Param {
    name: String,
    value: ParamValue,
}

impl Param {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn generate(&mut self) -> Value {
        self.value.generate()
    }

    fn initialize(spec: &ParamSpec, querier_id: QuerierId, querier_count: usize) -> Self {
        let name = spec.name.clone();
        let value = match &spec.param {
            ParamKind::Static(v) => ParamValue::Static(v.clone()),
            ParamKind::Cardinality { base, cardinality } => {
                let (min, max) = cardinality_range(*cardinality, querier_id, querier_count);
                ParamValue::Cardinality(CardinalityValue {
                    base: base.to_owned(),
                    min,
                    max,
                    current: min,
                })
            }
        };
        Self { name, value }
    }
}

fn cardinality_range(
    cardinality: usize,
    querier_id: QuerierId,
    querier_count: usize,
) -> (usize, usize) {
    let increment = usize::div_ceil(cardinality, querier_count);
    let min = querier_id * increment - increment + 1;
    let max = min + increment - 1;

    (min, max)
}

#[derive(Debug)]
pub enum ParamValue {
    Static(Value),
    Cardinality(CardinalityValue),
}

impl ParamValue {
    fn generate(&mut self) -> Value {
        match self {
            Self::Static(v) => v.clone(),
            Self::Cardinality(cv) => {
                let v = if let Some(base) = &cv.base {
                    format!("{base}{current}", current = cv.current)
                } else {
                    cv.current.to_string()
                };
                if cv.current > cv.max {
                    cv.current = cv.min;
                }
                Value::String(v)
            }
        }
    }
}

#[derive(Debug)]
pub struct CardinalityValue {
    base: Option<String>,
    min: usize,
    max: usize,
    current: usize,
}
