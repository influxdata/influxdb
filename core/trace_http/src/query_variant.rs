use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum QueryVariant {
    Sql,
    InfluxQl,
    FlightSql,
}

impl QueryVariant {
    pub fn str(&self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::InfluxQl => "influxql",
            Self::FlightSql => "flightsql",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryVariantExt(Arc<Mutex<Option<QueryVariant>>>);

impl QueryVariantExt {
    pub fn set(&self, variant: QueryVariant) {
        *self.0.lock() = Some(variant);
    }

    pub(crate) fn get(&self) -> Option<QueryVariant> {
        *self.0.lock()
    }
}
