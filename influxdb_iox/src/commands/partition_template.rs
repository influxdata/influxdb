use generated_types::influxdata::iox::partition_template::v1 as proto;
use snafu::{ResultExt, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Client Error: Invalid partition template format : {source}"))]
    InvalidPartitionTemplate { source: serde_json::Error },

    #[snafu(display("Client Error: Parts cannot be empty"))]
    NoParts,
}

#[derive(Debug, clap::Parser, Default, Clone)]
pub struct PartitionTemplateConfig {
    /// Partition template format:
    ///
    /// e.g. {"parts": [{"timeFormat": "%Y-%m"}, {"tagValue": "col1"}, {"tagValue": "col2,col3,col4"}]}
    ///
    ///  - timeFormat and tagValue can be in any order
    ///
    ///  - The value of timeFormat and tagValue are string and can be whatever at parsing time.
    ///    If they are not in the right format the server expcected, the server will return error.
    ///    Note that "time" is a reserved word and cannot be used in timeFormat.
    ///
    ///  - The number of timeFormats and tagValues are not limited at parsing time. Server limits
    ///    the total number of them and will send back error if it exceeds the limit 8.
    #[clap(
        action,
        long = "partition-template",
        short = 'p',
        default_value = None,
        value_parser = parse_partition_template,
    )]
    pub partition_template: Option<proto::PartitionTemplate>,
}

fn parse_partition_template(s: &str) -> Result<proto::PartitionTemplate, Error> {
    let part_template: proto::PartitionTemplate =
        serde_json::from_str(s).context(InvalidPartitionTemplateSnafu)?;

    // Error if empty parts
    if part_template.parts.is_empty() {
        return Err(Error::NoParts);
    }

    Ok(part_template)
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use test_helpers::assert_contains;

    use crate::commands::partition_template::PartitionTemplateConfig;
    use generated_types::influxdata::iox::partition_template::v1::template_part::Part;

    // ===================================================
    // Negative tests for parsing invalid partition template

    #[test]
    fn missing_partition_templat() {
        let error = PartitionTemplateConfig::try_parse_from(["server", "--partition-template"])
            .unwrap_err()
            .to_string();

        assert_contains!(error,
            "error: a value is required for '--partition-template <PARTITION_TEMPLATE>' but none was supplied"
        );
    }

    #[test]
    fn empty_partition_templat_time_format() {
        let partition_template =
            PartitionTemplateConfig::try_parse_from(["server", "--partition-template", ""])
                .unwrap_err()
                .to_string();

        assert_contains!(partition_template, "Client Error: Invalid partition template format : EOF while parsing a value at line 1 column 0");
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

        assert_contains!(partition_template, "Client Error: Parts cannot be empty");
    }

    #[test]
    fn wrong_time_format() {
        let partition_template = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"time Format\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert_contains!(partition_template, "Client Error: Invalid partition template format : unknown field `time Format`, expected one of `tag_value`, `tagValue`, `time_format`, `timeFormat`");
    }

    #[test]
    fn wrong_tag_format() {
        let partition_template = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"wrong format\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert_contains!(partition_template, "Client Error: Invalid partition template format : unknown field `wrong format`, expected one of `tag_value`, `tagValue`, `time_format`, `timeFormat`");
    }

    #[test]
    fn wrong_parts_format() {
        let partition_template = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"prts\": [{\"TagValue\": \"whatever\"}] }",
        ])
        .unwrap_err()
        .to_string();

        assert_contains!(partition_template, "Client Error: Invalid partition template format : unknown field `prts`, expected `parts`");
    }

    // ===================================================
    // Positive tests for parsing valid partition template

    #[test]
    fn valid_time_format() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"timeFormat\": \"whatever\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template.unwrap();
        assert_eq!(part_template.parts.len(), 1);
        assert_eq!(
            part_template.parts[0].part,
            Some(Part::TimeFormat("whatever".to_string()))
        );
    }

    #[test]
    fn valid_tag_format() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"tagValue\": \"whatever\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template.unwrap();
        assert_eq!(part_template.parts.len(), 1);
        assert_eq!(
            part_template.parts[0].part,
            Some(Part::TagValue("whatever".to_string()))
        );
    }

    #[test]
    fn valid_partition_template_time_first() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"timeFormat\": \"%Y.%j\"}, {\"tagValue\": \"col1\"}, {\"tagValue\": \"col2,col3 col4\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template.unwrap();
        assert_eq!(part_template.parts.len(), 3);
        assert_eq!(
            part_template.parts[0].part,
            Some(Part::TimeFormat("%Y.%j".to_string()))
        );
        assert_eq!(
            part_template.parts[1].part,
            Some(Part::TagValue("col1".to_string()))
        );
        assert_eq!(
            part_template.parts[2].part,
            Some(Part::TagValue("col2,col3 col4".to_string()))
        );
    }

    #[test]
    fn valid_partition_template_time_middle() {
        let actual = PartitionTemplateConfig::try_parse_from([
            "server",
            "--partition-template",
            "{\"parts\": [{\"tagValue\": \"col1\"}, {\"timeFormat\": \"%Y.%j\"}, {\"tagValue\": \"col2,col3 col4\"}] }",
        ])
        .unwrap();

        let part_template = actual.partition_template.unwrap();
        assert_eq!(part_template.parts.len(), 3);
        assert_eq!(
            part_template.parts[0].part,
            Some(Part::TagValue("col1".to_string()))
        );
        assert_eq!(
            part_template.parts[1].part,
            Some(Part::TimeFormat("%Y.%j".to_string()))
        );
        assert_eq!(
            part_template.parts[2].part,
            Some(Part::TagValue("col2,col3 col4".to_string()))
        );
    }
}
