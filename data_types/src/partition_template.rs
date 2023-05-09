use once_cell::sync::Lazy;
use std::sync::Arc;

/// A partition template specified by a namespace record.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NamespacePartitionTemplateOverride(PartitionTemplate);

impl NamespacePartitionTemplateOverride {
    /// Create a new, immutable override for a namespace's partition template.
    pub fn new(partition_template: PartitionTemplate) -> Self {
        Self(partition_template)
    }
}

/// A partition template specified by a table record.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TablePartitionTemplateOverride(PartitionTemplate);

impl TablePartitionTemplateOverride {
    /// Create a new, immutable override for a table's partition template.
    pub fn new(partition_template: PartitionTemplate) -> Self {
        Self(partition_template)
    }
}

/// A partition template specified as the default to be used in the absence of any overrides.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct DefaultPartitionTemplate(&'static PartitionTemplate);

impl Default for DefaultPartitionTemplate {
    fn default() -> Self {
        Self(&PARTITION_BY_DAY)
    }
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY: Lazy<PartitionTemplate> = Lazy::new(|| PartitionTemplate {
    parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
});

/// `PartitionTemplate` is used to compute the partition key of each row that gets written. It can
/// consist of a column name and its value or a formatted time. For columns that do not appear in
/// the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes what partition key
/// is generated.
#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(missing_docs)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

impl PartitionTemplate {
    /// If the table has a partition template, use that. Otherwise, if the namespace has a
    /// partition template, use that. If neither the table nor the namespace has a template,
    /// use the default template.
    pub fn determine_precedence<'a>(
        table: Option<&'a Arc<TablePartitionTemplateOverride>>,
        namespace: Option<&'a Arc<NamespacePartitionTemplateOverride>>,
        default: &'a DefaultPartitionTemplate,
    ) -> &'a PartitionTemplate {
        table
            .map(|t| &t.0)
            .or(namespace.map(|n| &n.0))
            .unwrap_or(default.0)
    }
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    /// The value in a named column
    Column(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
}
