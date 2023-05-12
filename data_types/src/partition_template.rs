use generated_types::{google::FieldViolation, influxdata::iox::partition_template::v1 as proto};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// A partition template specified by a namespace record.
#[derive(Debug, PartialEq, Clone)]
pub struct NamespacePartitionTemplateOverride(Arc<proto::PartitionTemplate>);

impl From<proto::PartitionTemplate> for NamespacePartitionTemplateOverride {
    fn from(partition_template: proto::PartitionTemplate) -> Self {
        Self(Arc::new(partition_template))
    }
}

impl Default for NamespacePartitionTemplateOverride {
    fn default() -> Self {
        Self(Arc::clone(&PARTITION_BY_DAY_PROTO))
    }
}

impl<DB> sqlx::Type<DB> for NamespacePartitionTemplateOverride
where
    sqlx::types::Json<Self>: sqlx::Type<DB>,
    DB: sqlx::Database,
{
    fn type_info() -> DB::TypeInfo {
        <sqlx::types::Json<Self> as sqlx::Type<DB>>::type_info()
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for NamespacePartitionTemplateOverride
where
    DB: sqlx::Database,
    for<'b> sqlx::types::Json<&'b proto::PartitionTemplate>: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        // Unambiguous delegation to the Encode impl on the Json type, which
        // exists due to the constraint in the where clause above.
        <sqlx::types::Json<&proto::PartitionTemplate> as sqlx::Encode<'_, DB>>::encode_by_ref(
            &sqlx::types::Json(&self.0),
            buf,
        )
    }
}

impl<'q, DB> sqlx::Decode<'q, DB> for NamespacePartitionTemplateOverride
where
    DB: sqlx::Database,
    sqlx::types::Json<proto::PartitionTemplate>: sqlx::Decode<'q, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'q>>::ValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <sqlx::types::Json<proto::PartitionTemplate> as sqlx::Decode<'_, DB>>::decode(value)?
                .0
                .into(),
        ))
    }
}

/// A partition template specified by a table record.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TablePartitionTemplateOverride(pub PartitionTemplate);

impl TablePartitionTemplateOverride {
    /// Create a new, immutable override for a table's partition template.
    pub fn new(partition_template: PartitionTemplate) -> Self {
        Self(partition_template)
    }
}

/// A partition template specified as the default to be used in the absence of any overrides.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct DefaultPartitionTemplate(&'static proto::PartitionTemplate);

impl Default for DefaultPartitionTemplate {
    fn default() -> Self {
        Self(&PARTITION_BY_DAY_PROTO)
    }
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY_PROTO: Lazy<Arc<proto::PartitionTemplate>> = Lazy::new(|| {
    Arc::new(proto::PartitionTemplate {
        parts: vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_owned(),
            )),
        }],
    })
});

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY: Lazy<Arc<PartitionTemplate>> =
    Lazy::new(|| Arc::new(PARTITION_BY_DAY_PROTO.as_ref().try_into().unwrap()));

impl TryFrom<&proto::PartitionTemplate> for PartitionTemplate {
    type Error = FieldViolation;

    fn try_from(value: &proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self {
            parts: value
                .parts
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<&proto::TemplatePart> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(value: &proto::TemplatePart) -> Result<Self, Self::Error> {
        let part = value
            .part
            .as_ref()
            .ok_or_else(|| FieldViolation::required(String::from("value")))?;

        Ok(match part {
            proto::template_part::Part::TagValue(value) => Self::TagValue(value.into()),
            proto::template_part::Part::TimeFormat(value) => Self::TimeFormat(value.into()),
        })
    }
}

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
        _table: Option<&'a Arc<TablePartitionTemplateOverride>>,
        _namespace: Option<&'a Arc<NamespacePartitionTemplateOverride>>,
        _default: &'a DefaultPartitionTemplate,
    ) -> &'a PartitionTemplate {
        unimplemented!()
    }
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    /// The value in the named tag, or the tag name if the value isn't present.
    TagValue(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
}
