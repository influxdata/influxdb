use generated_types::influxdata::iox::partition_template::v1 as proto;
use influxdb_iox_client::table::generated_types::PartitionTemplate;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Client Error: Invalid partition template format : {source}"))]
    InvalidPartitionTemplate { source: serde_json::Error },

    #[snafu(display("Client Error: Parts cannot be empty"))]
    NoParts,
}

/// Partition template in format:
/// {"parts": [{"TimeFormat": "%Y.%j"}, {"TagValue": "col1"}, {"TagValue": "col2,col3 col4"}] }
///  - TimeFormat and TagFormat can be in any order
///  - The value of TimeFormat and TagFormat are string and can be whatever at parsing time.
///    If they are not in the right format the server expcected, the server will return error.
///  - The number of TimeFormats and TagFormats are not limited at parsing time. Server limits
///    the total number of them and will send back error if it exceeds the limit.
#[derive(Debug, clap::Parser, Default, Clone)]
pub struct PartitionTemplateConfig {
    #[clap(
        action,
        long = "partition-template",
        short = 'p',
        default_value = None,
        value_parser = parse_part_template,
    )]
    pub parts: Option<PartTemplate>,
}

fn parse_part_template(s: &str) -> Result<PartTemplate, Error> {
    let part_template: PartTemplate =
        serde_json::from_str(s).context(InvalidPartitionTemplateSnafu)?;

    // Error if empty parts
    if part_template.parts.is_empty() {
        return Err(Error::NoParts);
    }

    Ok(part_template)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PartTemplate {
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Part {
    TagValue(String),
    TimeFormat(String),
}

impl PartitionTemplateConfig {
    pub fn partition_template(&self) -> Option<PartitionTemplate> {
        self.parts.as_ref()?;

        let part_template = self.parts.as_ref().unwrap().clone();

        let template_parts = part_template
            .parts
            .into_iter()
            .map(|part| match part {
                Part::TagValue(tag_value) => proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue(tag_value.clone())),
                },
                Part::TimeFormat(time_format) => proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat(time_format.clone())),
                },
            })
            .collect::<Vec<_>>();

        if template_parts.is_empty() {
            return None;
        }

        Some(PartitionTemplate {
            parts: template_parts,
        })
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::commands::partition_template::PartitionTemplateConfig;

    // ===================================================
    // Negative tests for parsing invalid partition template

    #[test]
    fn missing_partition_templat() {
        let error = PartitionTemplateConfig::try_parse_from(["server", "--partition-template"])
            .unwrap_err()
            .to_string();

        assert!(error.contains(
            "error: a value is required for '--partition-template <PARTS>' but none was supplied"
        ));
    }

    #[test]
    fn empty_partition_templat_time_format() {
        let partition_template =
            PartitionTemplateConfig::try_parse_from(["server", "--partition-template", ""])
                .unwrap_err()
                .to_string();

        assert!(partition_template.contains("Client Error: Invalid partition template format : EOF while parsing a value at line 1 column 0"));
    }

    #[test]
    fn empty_parts() {
        let partition_template = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": []}",
        ])
        .unwrap_err()
        .to_string();

        assert!(partition_template.contains("Client Error: Parts cannot be empty"));
    }

    #[test]
    fn wrong_time_format() {
        let partition_templat = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"timeFormat\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert!(partition_templat.contains("Client Error: Invalid partition template format : unknown variant `timeFormat`, expected `TagValue` or `TimeFormat` "));
    }

    #[test]
    fn wrong_tag_format() {
        let partition_templat = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"wrong format\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert!(partition_templat.contains("Client Error: Invalid partition template format : unknown variant `wrong format`, expected `TagValue` or `TimeFormat` "));
    }

    #[test]
    fn wrong_parts_format() {
        let partition_templat = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"prts\": [{\"TagValue\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert!(partition_templat
            .contains("Client Error: Invalid partition template format : missing field `parts`"));
    }

    // ===================================================
    // Positive tests for parsing valid partition template

    #[test]
    fn valid_time_format() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"TimeFormat\": \"whatever\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template().unwrap();
        assert_eq!(part_template.parts.len(), 1);
        assert_eq!(
            part_template.parts[0].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TimeFormat(
                "whatever".to_string()
            ))
        );
    }

    #[test]
    fn valid_tag_format() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"TagValue\": \"whatever\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template().unwrap();
        assert_eq!(part_template.parts.len(), 1);
        assert_eq!(
            part_template.parts[0].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "whatever".to_string()
            ))
        );
    }

    #[test]
    fn valid_partition_template_time_first() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"TimeFormat\": \"%Y.%j\"}, {\"TagValue\": \"col1\"}, {\"TagValue\": \"col2,col3 col4\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template().unwrap();
        assert_eq!(part_template.parts.len(), 3);
        assert_eq!(
            part_template.parts[0].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TimeFormat(
                "%Y.%j".to_string()
            ))
        );
        assert_eq!(
            part_template.parts[1].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "col1".to_string()
            ))
        );
        assert_eq!(
            part_template.parts[2].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "col2,col3 col4".to_string()
            ))
        );
    }

    #[test]
    fn valid_partition_template_time_middle() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"TagValue\": \"col1\"}, {\"TimeFormat\": \"%Y.%j\"}, {\"TagValue\": \"col2,col3 col4\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template().unwrap();
        assert_eq!(part_template.parts.len(), 3);
        assert_eq!(
            part_template.parts[0].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "col1".to_string()
            ))
        );
        assert_eq!(
            part_template.parts[1].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TimeFormat(
                "%Y.%j".to_string()
            ))
        );
        assert_eq!(
            part_template.parts[2].part,
            Some(generated_types::influxdata::iox::partition_template::v1::template_part::Part::TagValue(
                "col2,col3 col4".to_string()
            ))
        );
    }
}
