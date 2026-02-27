use datafusion::common::DataFusionError;
use influxdb_influxql_parser::expression::VarRef;
use influxdb_influxql_parser::visit::{Recursion, Visitor};
use std::collections::BTreeSet;

/// Visitor that collects the names of all the fields referenced in an expression.
#[derive(Debug)]
pub(super) struct SourceFieldNamesVisitor<'a>(pub(super) &'a mut BTreeSet<String>);

impl Visitor for SourceFieldNamesVisitor<'_> {
    type Error = DataFusionError;
    fn pre_visit_var_ref(self, varref: &VarRef) -> Result<Recursion<Self>, Self::Error> {
        if varref.data_type.is_some_and(|dt| dt.is_field_type()) {
            self.0.insert(varref.name.clone().take());
        }
        Ok(Recursion::Continue(self))
    }
}
