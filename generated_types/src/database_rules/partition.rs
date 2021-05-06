use std::convert::TryFrom;

use data_types::database_rules::{PartitionTemplate, RegexCapture, StrftimeColumn, TemplatePart};

use crate::google::protobuf::Empty;
use crate::google::{FieldViolation, FromFieldOpt, FromFieldString, FromFieldVec};
use crate::influxdata::iox::management::v1 as management;

impl From<PartitionTemplate> for management::PartitionTemplate {
    fn from(pt: PartitionTemplate) -> Self {
        Self {
            parts: pt.parts.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<management::PartitionTemplate> for PartitionTemplate {
    type Error = FieldViolation;

    fn try_from(proto: management::PartitionTemplate) -> Result<Self, Self::Error> {
        let parts = proto.parts.vec_field("parts")?;
        Ok(Self { parts })
    }
}

impl From<TemplatePart> for management::partition_template::part::Part {
    fn from(part: TemplatePart) -> Self {
        use management::partition_template::part::ColumnFormat;

        match part {
            TemplatePart::Table => Self::Table(Empty {}),
            TemplatePart::Column(column) => Self::Column(column),
            TemplatePart::RegexCapture(RegexCapture { column, regex }) => {
                Self::Regex(ColumnFormat {
                    column,
                    format: regex,
                })
            }
            TemplatePart::StrftimeColumn(StrftimeColumn { column, format }) => {
                Self::StrfTime(ColumnFormat { column, format })
            }
            TemplatePart::TimeFormat(format) => Self::Time(format),
        }
    }
}

impl TryFrom<management::partition_template::part::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::part::Part) -> Result<Self, Self::Error> {
        use management::partition_template::part::{ColumnFormat, Part};

        Ok(match proto {
            Part::Table(_) => Self::Table,
            Part::Column(column) => Self::Column(column.required("column")?),
            Part::Regex(ColumnFormat { column, format }) => Self::RegexCapture(RegexCapture {
                column: column.required("regex.column")?,
                regex: format.required("regex.format")?,
            }),
            Part::StrfTime(ColumnFormat { column, format }) => {
                Self::StrftimeColumn(StrftimeColumn {
                    column: column.required("strf_time.column")?,
                    format: format.required("strf_time.format")?,
                })
            }
            Part::Time(format) => Self::TimeFormat(format.required("time")?),
        })
    }
}

impl From<TemplatePart> for management::partition_template::Part {
    fn from(part: TemplatePart) -> Self {
        Self {
            part: Some(part.into()),
        }
    }
}

impl TryFrom<management::partition_template::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::Part) -> Result<Self, Self::Error> {
        proto.part.required("part")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::database_rules::DatabaseRules;
    use std::convert::TryInto;

    #[test]
    fn test_partition_template_default() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate { parts: vec![] }),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.partition_template.parts.len(), 0);
        assert_eq!(protobuf.partition_template, back.partition_template);
    }

    #[test]
    fn test_partition_template_no_part() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate {
                parts: vec![Default::default()],
            }),
            ..Default::default()
        };

        let res: Result<DatabaseRules, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "partition_template.parts.0.part");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_partition_template() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![
                management::partition_template::Part {
                    part: Some(Part::Time("time".to_string())),
                },
                management::partition_template::Part {
                    part: Some(Part::Table(Empty {})),
                },
                management::partition_template::Part {
                    part: Some(Part::Regex(ColumnFormat {
                        column: "column".to_string(),
                        format: "format".to_string(),
                    })),
                },
            ],
        };

        let pt: PartitionTemplate = protobuf.clone().try_into().unwrap();
        let back: management::PartitionTemplate = pt.clone().into();

        assert_eq!(
            pt.parts,
            vec![
                TemplatePart::TimeFormat("time".to_string()),
                TemplatePart::Table,
                TemplatePart::RegexCapture(RegexCapture {
                    column: "column".to_string(),
                    regex: "format".to_string()
                })
            ]
        );
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_partition_template_empty() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![management::partition_template::Part {
                part: Some(Part::Regex(ColumnFormat {
                    ..Default::default()
                })),
            }],
        };

        let res: Result<PartitionTemplate, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "parts.0.part.regex.column");
        assert_eq!(&err.description, "Field is required");
    }
}
